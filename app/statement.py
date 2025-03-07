'''
    This module provides all the function about google analytics and bigquery.
    Help you get the information to create the financial statements.
'''
import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    Filter, 
    FilterExpression,
    FilterExpressionList
)
from google.cloud import bigquery as bq
from datetime import datetime, timezone
import math
from openpyxl import Workbook
from openpyxl.styles import PatternFill
from dateutil.relativedelta import relativedelta
from app.gql import gql_query
from google.ads import admanager_v1
from google.auth import default
from google.auth.transport.requests import Request
from google.type.date_pb2 import Date
from app.config import ADSENSE_EXPECTED_RATIO, GAM_EXPECTED_RATIO

GAM_REVENUE_PARTIAL = 0.85 # How much of the revenue goes to publisher's revenue

adsense_homepage_title = "READr Mesh 讀選"
adsense_newpage_title  = "最新 | READr Mesh 讀選"
adsense_socialpage_title = "社群 | READr Mesh 讀選"
gam_home_title    = "home"
gam_social_title  = "social"
gam_article_title = "article"
gam_profile_title = "profile"

gql_sponsorships = '''
    query sponsorships{{
      sponsorships(where: {{status: {{equals: Success}}, createdAt: {{gt: "{START_TIME}" }} }}){{
        id
    	publisher{{
          id
        }}
        fee
      }}
    }}
'''

gql_statement_publishers = '''
query Publishers{
  publishers(where: {is_active: {equals: true}}){
    id
    title
    customId
    full_content
  }
}
'''

gql_create_statements = '''
mutation createStatements($data: [StatementCreateInput!]!){
  createStatements(data: $data){
    id
  }
}
'''

gql_create_revenues = '''
mutation createRevenues($data: [RevenueCreateInput!]!){
  createRevenues(data: $data){
    id
  }
}
'''

gql_query_exchanges = '''
query exchanges{{
  exchanges(where: {{createdAt: {{gte: "{START_DATE}" }}, status: {{equals: Success}} }}, orderBy: {{id: desc}}){{
    publisher{{
      id
    }}
    tid
    exchangeVolume
    createdAt
  }}
}}
'''

gql_query_revenues = '''
query revenues{{
  revenues(where: {{createdAt: {{gte: "{START_DATE}" }}, type: {{in: [story_ad_revenue]}} }}, orderBy: {{id: desc}}){{
    publisher{{
      id
    }}
    type
    value
    createdAt
    start_date
  }}
}}
'''

gql_members_balance = """
query members{
  members(where: {wallet: {not: {equals: ""}}}){
    id
    name
    balance
    publisher{
      id
      title
    }
  }
}
"""

gql_publishers_balance = """
query publishers{
  publishers(where: {is_active: {equals: true}}){
    id
    title
    admin{
      balance
    }
  }
}
"""

def to_google_date(dt):
    return Date(year=dt.year, month=dt.month, day=dt.day)

def getAdsenseRevenues(ga_resource_id, start_datetime, end_datetime, expected_ratio: float=ADSENSE_EXPECTED_RATIO):
    '''
        In this function, we will get revenues from previous months.
        For example, when ga_months=1, and current date is 2024-10-05,
        we will get revenues from 2024-09-01 to 2024-09-30.
    '''
    # setup ga days
    start_date = datetime.strftime(start_datetime, '%Y-%m-%d')
    end_date = datetime.strftime(end_datetime, '%Y-%m-%d')
    
    # setup filter criteria
    filter_criteria = FilterExpression(
        or_group=FilterExpressionList(
            expressions=[
                FilterExpression(
                    filter=Filter(
                        field_name="pageTitle",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value=adsense_homepage_title
                        )
                    )
                ),
                FilterExpression(
                    filter=Filter(
                        field_name="pageTitle",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value=adsense_newpage_title
                        )
                    )
                ),
                FilterExpression(
                    filter=Filter(
                        field_name="pageTitle",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value=adsense_socialpage_title
                        )
                    )
                )
            ]
        )
    )

    # send request
    request = RunReportRequest(
        property=f"properties/{ga_resource_id}",
        dimensions=[
            Dimension(name="pageTitle"),
        ],
        metrics=[
            Metric(name="totalAdRevenue"),
        ],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimension_filter=filter_criteria,
    )
    client = BetaAnalyticsDataClient()
    response = client.run_report(request)

    # parse response
    revenue_table = {}
    total_revenue = 0
    for row in response.rows:
        dimension_value = str(row.dimension_values[0].value)
        metric_value = expected_ratio*float(row.metric_values[0].value)
        revenue_table[dimension_value] = metric_value
        total_revenue += metric_value
    revenue_table['total'] = total_revenue
    return revenue_table

def getPublisherPageview(db_name: str, table_name: str, start_time: str):
    QUERY = (
        f'SELECT jsonPayload.complementary.targetid, COUNT(jsonPayload.complementary.targetid) AS view FROM `{db_name}.{table_name}` '
        f'WHERE (resource.type="global") AND (jsonPayload.type="click-story" OR jsonPayload.type="click-related-story") AND (jsonPayload.complementary.publishertarget="publisher") AND timestamp >= TIMESTAMP("{start_time}")'
        'GROUP BY jsonPayload.complementary.targetid;'
    )
    client = bq.Client()
    rows = client.query(QUERY).result()
    
    # parse response, pv_table contains {publisher_id: pv_count} relationship
    pv_table = {}
    for row in rows:
        pv_table[row.targetid] = row.view
    return pv_table

def calculateMutualFund(homepage_revenue: float, newpage_revenue: float):
    '''
        共同基金池資金 = 首頁廣告收益0.2+最新頁面廣告收益0.05
    '''
    return homepage_revenue*0.2+newpage_revenue*0.05

def calculatePlatformIncome(homepage_revenue: float, homesubpage_revenue: float, newpage_revenue: float, socialpage_revenue: float, collection_ad_revenue: float, article_ad_revenue: float):
    '''
        平台收益 = (首頁收益+首頁子頁收益)*0.5 + 最新頁面收益*0.5 + 社群頁面收益*0.85*0.5 + 集錦廣告收益*0.85*0.5 + 文章廣告收益*0.85*0.45
    '''
    total_revenue = (homepage_revenue+homesubpage_revenue)*0.5 + newpage_revenue*0.5 + socialpage_revenue*0.85*0.5 + collection_ad_revenue*0.85*0.5 + article_ad_revenue*0.85*0.45
    return total_revenue

def publisherSponsorshipShare(gql_endpoint, mutual_fund, start_time):
    # fetch data
    data = gql_query(gql_endpoint, gql_sponsorships.format(START_TIME=start_time))
    sponsorships = data['sponsorships']

    # calculate statistic from sponsorships
    sponsor_table = {}
    total_fee = 0
    for sponsorship in sponsorships:
        publisher_id = sponsorship['publisher']['id']
        fee = sponsorship['fee']
        sponsor_table[publisher_id] = sponsor_table.get(publisher_id, 0)+fee
        total_fee += fee
    publisher_share_table = { pid: (fee/total_fee)*mutual_fund for pid, fee in sponsor_table.items() }
    return publisher_share_table

def createRevenuesData(gql_endpoint, shares_table: dict, start_date: str, end_date: str):
    var_revenues = {
        "data": []
    }
    for pid, data in shares_table.items():
        title, sponsorship_share, pv_share = data['title'], data['sponsorship_share'], data['pv_share']
        var_revenues["data"].append({
            "publisher": {
                "connect": {
                    "id": pid
                }
            },
            "title": f"{title}基金池分潤",
            "type": "mutual_fund_revenue",
            "value": sponsorship_share,
            "start_date": start_date,
            "end_date": end_date
        })
        var_revenues["data"].append({
            "publisher": {
                "connect": {
                    "id": pid
                }
            },
            "title": f"{title}廣告分潤",
            "type": "story_ad_revenue",
            "value": pv_share,
            "start_date": start_date,
            "end_date": end_date
        })
    data = gql_query(gql_endpoint, gql_create_revenues, var_revenues)
    return data

def createMonthStatement(start_date: str, end_date: str, gql_endpoint: str, adsense_total_revenue: float, gam_total_revenue: float, gam_article_revenue: float, mesh_income: float, mutual_fund: float, user_points: int, publisher_share_table: dict, pv_table, adsense_complementary: str="", gam_complementary: str="", point_complementary: str=""):
    wb = Workbook()
    ws = wb.active
    current_time = datetime.now()
    date = current_time.strftime("%Y-%m-%d")
    
    # style setting
    ws.column_dimensions["A"].width = 20
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 40
    ws.column_dimensions["D"].width = 20
    orange_fill = PatternFill(start_color="FFA500", end_color="FFA500", fill_type="solid")
    precision = "{:.3f}"
    
    # for all the profit
    start_row = 1
    ws.merge_cells(f"A{start_row}:C{start_row}")
    ws[f'A{start_row}'] = f"報表區間: {start_date}-{end_date}"
    
    start_row = 2
    ws.merge_cells(f"A{start_row}:C{start_row}")
    ws[f"A{start_row}"].fill = orange_fill
    ws[f'A{start_row}'] = "收益總覽"
    ws[f'A{start_row+1}'], ws[f'B{start_row+1}'], ws[f'C{start_row+1}'] = "項目", "金額(TWD)", "備註"
    ws[f'A{start_row+2}'], ws[f'B{start_row+2}'], ws[f'C{start_row+2}'] = "Adsense收益", precision.format(adsense_total_revenue), adsense_complementary
    ws[f'A{start_row+3}'], ws[f'B{start_row+3}'], ws[f'C{start_row+3}'] = "GAM收益", precision.format(gam_total_revenue), gam_complementary
    ws[f'A{start_row+4}'], ws[f'B{start_row+4}'] = "總收益", precision.format(adsense_total_revenue + gam_total_revenue)

    # for income
    income_start_row = start_row+6
    ws.merge_cells(f"A{income_start_row}:C{income_start_row}")
    ws[f"A{income_start_row}"].fill = orange_fill
    ws[f'A{income_start_row}'] = "平台收入"
    ws[f'A{income_start_row+1}'], ws[f'B{income_start_row+1}'], ws[f'C{income_start_row+1}'] = "項目", "金額(TWD)", "備註"
    ws[f'A{income_start_row+2}'], ws[f'B{income_start_row+2}'] = "Mesh平台收入", precision.format(mesh_income)

    # for mutual fund
    fund_start_row = income_start_row+4
    ws.merge_cells(f"A{fund_start_row}:C{fund_start_row}")
    ws[f"A{fund_start_row}"].fill = orange_fill
    ws[f'A{fund_start_row}'] = "媒體共同基金池"
    ws[f'A{fund_start_row+1}'], ws[f'B{fund_start_row+1}'], ws[f'C{fund_start_row+1}'] = "項目", "金額(TWD)", "點數(MSP)"
    ws[f'A{fund_start_row+2}'], ws[f'B{fund_start_row+2}'], ws[f'C{fund_start_row+2}'] = "共同基金池", precision.format(mutual_fund), math.floor(mutual_fund)

    # user all points
    point_start_row = fund_start_row+4
    ws.merge_cells(f"A{point_start_row}:C{point_start_row}")
    ws[f"A{point_start_row}"].fill = orange_fill
    ws[f'A{point_start_row}'] = "用戶點數"
    ws[f'A{point_start_row+1}'], ws[f'B{point_start_row+1}'], ws[f'C{point_start_row+1}'] = "項目", "點數(MSP)", "備註"
    ws[f'A{point_start_row+2}'], ws[f'B{point_start_row+2}'], ws[f'C{point_start_row+2}'] = "點數總合", user_points, point_complementary

    # for publisher shares
    publisher_start_row = point_start_row+4
    ws.merge_cells(f"A{publisher_start_row}:C{publisher_start_row}")
    ws[f"A{publisher_start_row}"].fill = orange_fill
    ws[f'A{publisher_start_row}'] = "媒體廣告分潤"
    ws[f'A{publisher_start_row+1}'], ws[f'B{publisher_start_row+1}'], ws[f'C{publisher_start_row+1}'], ws[f'D{publisher_start_row+1}'] = "媒體名稱", "共同基金池分潤(TWD)", "文章頁廣告分潤(TWD)", "瀏覽次數(PV)"

    data = gql_query(gql_endpoint, gql_statement_publishers)
    publishers = data['publishers']
    total_pv = sum(pv_table.values())
    if total_pv==0:
        total_pv = 1 # avoid divide by 0 issue
    index = publisher_start_row+2
    
    shares_table = {}
    for idx, publisher in enumerate(publishers):
        id, title, full_content = publisher['id'], publisher['title'], publisher['full_content']
        sponsorship_share = publisher_share_table.get(str(id), 0.0)
        publisher_pv = pv_table.get(str(id), 0.0)
        pv_share = (publisher_pv/total_pv)*gam_article_revenue*GAM_REVENUE_PARTIAL if full_content==True else 0
        ws[f'A{index+idx}'], ws[f'B{index+idx}'], ws[f'C{index+idx}'], ws[f'D{index+idx}'] = title, precision.format(sponsorship_share), precision.format(pv_share), publisher_pv
        shares_table[id] = {
            "title": title,
            "sponsorship_share": sponsorship_share,
            "pv_share": pv_share
        }
    createRevenuesData(
        gql_endpoint = gql_endpoint, 
        shares_table = shares_table, 
        start_date = start_date, 
        end_date = end_date
    )
    
    # file
    folder = os.path.join("statements", "general")
    filename = os.path.join(folder, f"monthly-statement-{date}.xlsx")
    if not os.path.exists(folder):
        os.makedirs(folder)
    wb.save(filename)
    return filename


def createMediaStatements(gql_endpoint: str, domain: str, start_date: str, end_date: str, charge_percent: float=0.1):
    current_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    date = current_time.strftime("%Y%m%d")
    filenames = []
    
    # prefetching the necessary data
    data = gql_query(gql_endpoint, gql_statement_publishers)
    publishers = data['publishers']
    
    data = gql_query(gql_endpoint, gql_query_exchanges.format(START_DATE=start_date))
    exchanges = data['exchanges']
    exchange_table = {} # mapping publisher_id to exchange records
    for exchange in exchanges:
        pid = exchange['publisher']['id']
        exchange_list = exchange_table.setdefault(pid, [])
        exchange_list.append(exchange)

    data = gql_query(gql_endpoint, gql_query_revenues.format(START_DATE=start_date))
    revenues = data['revenues']
    revenue_table = {}
    for revenue in revenues:
        pid = revenue['publisher']['id']
        revenue_list = revenue_table.setdefault(pid, [])
        revenue_list.append(revenue)
    
    # data processing
    var_statements = {
        "data": []
    }
    for publisher in publishers:
        pid, customId, _ = publisher['id'], publisher['customId'], publisher['title']
        folder = os.path.join("statements", "media", customId)
        if not os.path.exists(folder):
            os.makedirs(folder)
        filename = os.path.join(folder, f"{customId}_{date}.xlsx")
        
        # excel: global setting
        wb = Workbook()
        ws = wb.active
        ws.column_dimensions["A"].width = 40
        ws.column_dimensions["B"].width = 70
        ws.column_dimensions["C"].width = 20
        ws.column_dimensions["D"].width = 20
        ws.column_dimensions["E"].width = 20
        ws.column_dimensions["F"].width = 20
        
        # excel: title
        start_row = 1
        ws.merge_cells(f"A{start_row}:F{start_row}")
        ws[f'A{start_row}'] = f"報表區間: {start_date}-{end_date}"
        ws[f'A{start_row+1}'], ws[f'B{start_row+1}'], ws[f'C{start_row+1}'] = "建立日期", "金流編號", "項目"
        ws[f'D{start_row+1}'], ws[f'E{start_row+1}'], ws[f'F{start_row+1}'] = "收取金額", "手續費", "實際收取金額"
        
        # excel: add exchanges information
        item_row = start_row+2 
        publisher_exchanges = exchange_table.get(pid, [])
        for exchange in publisher_exchanges:
            tid = exchange['tid']
            exchangeVolume = exchange['exchangeVolume']
            charge = math.ceil(exchangeVolume*charge_percent)
            createdAt = exchange['createdAt']
            ws[f'A{item_row}'], ws[f'B{item_row}'], ws[f'C{item_row}'] = createdAt, tid, "點數兌換"
            ws[f'D{item_row}'], ws[f'E{item_row}'], ws[f'F{item_row}'] = exchangeVolume, charge, (exchangeVolume-charge)
            item_row += 1
        publisher_revenues = revenue_table.get(pid, [])
        for revenue in publisher_revenues:
            type_name = revenue['type']
            if type_name != "story_ad_revenue":
                continue
            type_name = "廣告收益"
            createdAt = revenue['createdAt']
            value = revenue['value']
            revenue_start_date = revenue['start_date']
            month = datetime.strptime(revenue_start_date, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%m')
            item_name = f"{month}月{type_name}"
            
            ws[f'A{item_row}'], ws[f'B{item_row}'], ws[f'C{item_row}'] = createdAt, "", item_name
            ws[f'D{item_row}'], ws[f'E{item_row}'], ws[f'F{item_row}'] = value, 0, value
            item_row += 1
            
        # file processing
        wb.save(filename)
        filenames.append(filename)
        var_statements["data"].append({
            "title": f"{customId}_{date}",
            "type": "media",
            "url": f"{domain}{filename}",
            "publisher": {
                "connect": {
                    "id": pid
                }
            },
            "start_date": start_date,
            "end_date": end_date,
        })
        
    # update CMS
    gql_query(gql_endpoint, gql_create_statements, var_statements)
    return filenames

def semiAnnualStatement(gql_endpoint: str, months: int=6):
    current_time = datetime.now(timezone.utc).replace(days=1, hour=0, minute=0, second=0, microsecond=0)
    date = current_time.strftime("%Y-%m-%d")

    # prefetching the necessary data
    data = gql_query(gql_endpoint, gql_members_balance)
    members = data['members']
    data = gql_query(gql_endpoint, gql_publishers_balance)
    publishers = data['publishers']

    # excel design
    folder = os.path.join("statements", "general")
    if not os.path.exists(folder):
        os.makedirs(folder)
    filename = os.path.join(folder, f"semi-annual-statement-{date}.xlsx")
    
    start_date = (current_time - relativedelta(months=months)).isoformat().replace('+00:00', 'Z')
    end_date = current_time.isoformat().replace('+00:00', 'Z')
    
    # excel: global setting
    wb = Workbook()
    ws = wb.active
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 30
    ws.column_dimensions["C"].width = 50
    orange_fill = PatternFill(start_color="FFA500", end_color="FFA500", fill_type="solid")

    # excel: publisher balances
    start_row = 1
    ws.merge_cells(f"A{start_row}:C{start_row}")
    ws[f'A{start_row}'] = f"報表區間: {start_date}-{end_date}"
    start_row += 1
    ws.merge_cells(f"A{start_row}:C{start_row}")
    ws[f'A{start_row}'].fill = orange_fill
    ws[f'A{start_row}'] = f"媒體點數"
    start_row += 1
    ws[f'A{start_row}'], ws[f'B{start_row}'], ws[f'C{start_row}'] = "名稱", "點數", "備註"

    for publisher in publishers:
        start_row += 1
        title = publisher['title']
        balance = publisher['admin']['balance'] if publisher['admin'] else 0
        ws[f'A{start_row}'], ws[f'B{start_row}'], ws[f'C{start_row}'] = title, balance, ""

    # excel: member balances
    start_row += 3
    ws.merge_cells(f"A{start_row}:C{start_row}")
    ws[f'A{start_row}'].fill = orange_fill
    ws[f'A{start_row}'] = f"用戶點數"
    start_row += 1
    ws[f'A{start_row}'], ws[f'B{start_row}'], ws[f'C{start_row}'] = "名稱", "點數", "備註"

    for member in members:
        start_row += 1
        name = member['name']
        balance = member['balance']
        
        note = ""
        manage_publishers = member['publisher']
        if len(manage_publishers)>0:
            note = "為媒體管理員: "
            for publisher in manage_publishers:
                title = publisher['title']
                note += f"{title}, "
        ws[f'A{start_row}'], ws[f'B{start_row}'], ws[f'C{start_row}'] = name, balance, note

    # save file
    wb.save(filename)
    print("Successfully save statement: ", filename)
    return filename

def getTotalPoints(gql_endpoint):
    data = gql_query(gql_endpoint, gql_members_balance)
    members = data['members']
    total_points = 0
    for member in members:
        total_points += member.get('balance', 0)
    return total_points

# GAM revenue related functions
def getGamRevenues(network_code, start_datetime, end_datetime, expected_ratio: float=GAM_EXPECTED_RATIO):
    # Refresh scope
    scopes = ["https://www.googleapis.com/auth/admanager"]
    credentials, _ = default(scopes=scopes)
    credentials.refresh(Request())
    
    # Create report
    revenue_table = {
        gam_home_title: 0.0,
        gam_social_title: 0.0,
        gam_article_title: 0.0,
        gam_profile_title: 0.0,
    }
    client = admanager_v1.ReportServiceClient(credentials=credentials)

    report = admanager_v1.Report()
    report.report_definition.dimensions = ['AD_UNIT_CODE']
    report.report_definition.metrics = ['REVENUE']
    report.report_definition.report_type = "HISTORICAL"
    report.report_definition.date_range = admanager_v1.types.Report.DateRange(
        fixed = admanager_v1.types.Report.DateRange.FixedDateRange(
            start_date=to_google_date(start_datetime),
            end_date=to_google_date(end_datetime)
        )
    )

    request = admanager_v1.CreateReportRequest(
        parent=f"networks/{network_code}",
        report=report,
    )
    response = client.create_report(request=request)
    
    # Run report
    report_id = response.report_id
    request = admanager_v1.RunReportRequest(
        name=f"networks/{network_code}/reports/{report_id}",
    )
    operation = client.run_report(request=request)
    print("Waiting for operation to complete...")
    response = operation.result()

    # Fetch report result
    report_result = response.report_result
    request = admanager_v1.FetchReportResultRowsRequest(
        name=report_result
    )
    revenues = client.fetch_report_result_rows(request=request)
    for revenue in revenues:
        ad_name = revenue.dimension_values[0].string_value
        dollor  = revenue.metric_value_groups[0].primary_values[0].double_value
        if "mmesh_home" in ad_name:
            revenue_table[gam_home_title] += dollor
        if "mmesh_social" in ad_name:
            revenue_table[gam_social_title] += dollor
        if "mmesh_profile" in ad_name:
            revenue_table[gam_profile_title] += dollor
        if "mmesh_article" in ad_name:
            revenue_table[gam_article_title] += dollor
    for key, value in revenue_table.items():
        revenue_table[key] = value * expected_ratio
    revenue_table['total'] = sum(revenue_table.values())
    return revenue_table