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
from datetime import datetime, timedelta
import math
from openpyxl import Workbook
from openpyxl.styles import PatternFill

homepage_title = "READr Mesh 讀選"
newpage_title  = "最新 | READr Mesh 讀選"
socialpage_title = "社群 | READr Mesh 讀選"

def getRevenues(ga_resource_id, ga_days):
    # setup ga days
    current_time = datetime.now()
    start_datetime = (current_time - timedelta(days=ga_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = datetime.strftime(start_datetime, '%Y-%m-%d')
    
    # setup filter criteria
    filter_criteria = FilterExpression(
        or_group=FilterExpressionList(
            expressions=[
                FilterExpression(
                    filter=Filter(
                        field_name="pageTitle",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value=homepage_title
                        )
                    )
                ),
                FilterExpression(
                    filter=Filter(
                        field_name="pageTitle",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value=newpage_title
                        )
                    )
                ),
                FilterExpression(
                    filter=Filter(
                        field_name="pageTitle",
                        string_filter=Filter.StringFilter(
                            match_type=Filter.StringFilter.MatchType.EXACT,
                            value=socialpage_title
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
            Metric(name="totalAdRevenue"),  # 使用者
        ],
        date_ranges=[DateRange(start_date=start_date, end_date="today")],
        dimension_filter=filter_criteria,
    )
    client = BetaAnalyticsDataClient()
    response = client.run_report(request)

    # parse response
    revenue_table = {}
    total_revenue = 0
    for row in response.rows:
        dimension_value = str(row.dimension_values[0].value)
        metric_value = float(row.metric_values[0].value)
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

def calculatePlatformIncome(homepage_revenue: float, newpage_revenue: float, socialpage_revenue: float, collection_ad_revenue: float, story_ad_revenue: float):
    '''
        平台收益 = (首頁廣告收益0.5+最新廣告收益0.5+社群廣告收益0.5)+(集錦廣告收益0.5+文章廣告收益*0.45)
    '''
    return (homepage_revenue+newpage_revenue+socialpage_revenue)*0.5+(collection_ad_revenue*0.5+story_ad_revenue*0.45)

def createMontlyStatement(adsense_revenue: float, gam_revenue: float, mesh_income: float, mutual_fund: float, user_points: int, pv_table, adsense_complementary: str="", gam_complementary: str="", point_complementary: str=""):
    wb = Workbook()
    ws = wb.active
    current_time = datetime.now()
    date = current_time.strftime("%Y-%m-%d")
    
    # style setting
    ws.column_dimensions["A"].width = 20
    ws.column_dimensions["B"].width = 20
    ws.column_dimensions["C"].width = 40
    orange_fill = PatternFill(start_color="FFA500", end_color="FFA500", fill_type="solid")
    precision = "{:.3f}"
    
    # for all the profit
    start_row = 1
    ws.merge_cells(f"A{start_row}:C{start_row}")
    ws[f"A{start_row}"].fill = orange_fill
    ws[f'A{start_row}'] = "收益總覽"
    ws[f'A{start_row+1}'], ws[f'B{start_row+1}'], ws[f'C{start_row+1}'] = "項目", "金額(TWD)", "備註"
    ws[f'A{start_row+2}'], ws[f'B{start_row+2}'], ws[f'C{start_row+2}'] = "Adsense收益", precision.format(adsense_revenue), adsense_complementary
    ws[f'A{start_row+3}'], ws[f'B{start_row+3}'], ws[f'C{start_row+3}'] = "GAM收益", precision.format(gam_revenue), gam_complementary
    ws[f'A{start_row+4}'], ws[f'B{start_row+4}'] = "總收益", precision.format(adsense_revenue + gam_revenue)

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

    folder = os.path.join(".", "statements", "general")
    filename = os.path.join(folder, f"monthly-statement-{date}.xlsx")
    if not os.path.exists(folder):
        os.makedirs(folder)
    wb.save(filename)
    return filename