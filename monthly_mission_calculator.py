#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from db_connector import PostgreSQLConnector
from datetime import datetime, timedelta
import calendar
import json
import os
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

class MonthlyMissionCalculator:
    def __init__(self):
        self.db = PostgreSQLConnector()
    
    def connect(self):
        """데이터베이스 연결"""
        return self.db.connect()
    
    def disconnect(self):
        """데이터베이스 연결 해제"""
        self.db.disconnect()
    
    def get_previous_month_data(self, year, month):
        """이전 달의 유산소 기록 데이터 조회"""
        # 이전 달 계산
        if month == 1:
            prev_year = year - 1
            prev_month = 12
        else:
            prev_year = year
            prev_month = month - 1
        
        start_date = f"{prev_year}-{prev_month:02d}-01"
        if prev_month == 12:
            end_date = f"{prev_year + 1}-01-01"
        else:
            end_date = f"{prev_year}-{prev_month + 1:02d}-01"
        
        # 사용자별 일별 총 거리만 계산 (지점 중복 제거)
        query = f"""
        SELECT
            TO_CHAR(DATE_TRUNC('day', a.start_datetime), 'YYYY-MM-DD') AS 운동일,
            TO_CHAR(DATE_TRUNC('day', a.start_datetime), 'Dy') AS 요일,
            a.user_id AS 사용자_id,
            u.name AS 사용자_이름,
            -- 지점이 여러개면 첫 번째 지점만 사용
            (SELECT b2.name FROM b_class_bplace b2 WHERE b2.id = a.b_place_id LIMIT 1) AS 운동장소,
            ROUND(SUM(CASE 
                WHEN a.device_type = 'treadmill' THEN a.distance
                WHEN a.device_type = 'cycle' THEN a.distance * 0.4
                WHEN a.device_type = 'rowing' THEN a.distance * 0.7
                ELSE 0 END) / 1000.00, 2) AS 총_운동_거리_km
        FROM
            b_class_userplaylog a
        LEFT JOIN
            user_user u ON u.id = a.user_id
        WHERE
            a.start_datetime >= '{start_date}'
            AND a.start_datetime < '{end_date}'
        GROUP BY
            DATE_TRUNC('day', a.start_datetime), a.user_id, u.name, a.b_place_id
        ORDER BY
            운동일, 사용자_id;
        """
        
        result = self.db.execute_query(query)
        return result
    
    def get_branch_weekday_data(self, year, month):
        """지점별 요일별 데이터 조회"""
        # 이전 달 계산
        if month == 1:
            prev_year = year - 1
            prev_month = 12
        else:
            prev_year = year
            prev_month = month - 1
        
        start_date = f"{prev_year}-{prev_month:02d}-01"
        if prev_month == 12:
            end_date = f"{prev_year + 1}-01-01"
        else:
            end_date = f"{prev_year}-{prev_month + 1:02d}-01"
        
        # 지점별 요일별 데이터 조회 (간단한 방식으로 수정)
        query = f"""
        SELECT
            COALESCE(b.name, '미지정') AS 지점명,
            TO_CHAR(DATE_TRUNC('day', a.start_datetime), 'Dy') AS 요일,
            COUNT(DISTINCT a.user_id) AS 사용자수,
            ROUND(SUM(CASE 
                WHEN a.device_type = 'treadmill' THEN a.distance
                WHEN a.device_type = 'cycle' THEN a.distance * 0.4
                WHEN a.device_type = 'rowing' THEN a.distance * 0.7
                ELSE 0 END) / 1000.00, 2) AS 총_운동_거리_km,
            ROUND(AVG(CASE 
                WHEN a.device_type = 'treadmill' THEN a.distance
                WHEN a.device_type = 'cycle' THEN a.distance * 0.4
                WHEN a.device_type = 'rowing' THEN a.distance * 0.7
                ELSE 0 END) / 1000.00, 2) AS 평균_운동_거리_km
        FROM
            b_class_userplaylog a
        LEFT JOIN
            b_class_bplace b ON b.id = a.b_place_id
        WHERE
            a.start_datetime >= '{start_date}'
            AND a.start_datetime < '{end_date}'
        GROUP BY
            b.name, TO_CHAR(DATE_TRUNC('day', a.start_datetime), 'Dy')
        ORDER BY
            b.name, 
            CASE TO_CHAR(DATE_TRUNC('day', a.start_datetime), 'Dy')
                WHEN 'Mon' THEN 1
                WHEN 'Tue' THEN 2
                WHEN 'Wed' THEN 3
                WHEN 'Thu' THEN 4
                WHEN 'Fri' THEN 5
                WHEN 'Sat' THEN 6
                WHEN 'Sun' THEN 7
            END;
        """
        
        result = self.db.execute_query(query)
        return result
    
    def calculate_weekday_averages(self, data):
        """요일별 평균 거리 계산"""
        if not data or 'data' not in data:
            return {}
        
        weekday_totals = {}
        weekday_days = {}  # 요일별 실제 일수
        
        for row in data['data']:
            weekday = row[1]  # 요일
            distance = float(row[5]) if row[5] else 0  # 총_운동_거리_km
            date = row[0]  # 운동일
            
            if weekday not in weekday_totals:
                weekday_totals[weekday] = 0
                weekday_days[weekday] = set()
            
            weekday_totals[weekday] += distance
            weekday_days[weekday].add(date)  # 해당 요일의 날짜들 수집
        
        weekday_averages = {}
        for weekday in weekday_totals:
            if len(weekday_days[weekday]) > 0:
                # 요일별 총 거리를 해당 요일의 실제 일수로 나누어 일평균 계산
                weekday_averages[weekday] = weekday_totals[weekday] / len(weekday_days[weekday])
        
        return weekday_averages
    
    def get_business_days(self, year, month):
        """특정 월의 영업일수 계산 (일요일 격주 운영, 공휴일 제외)"""
        # 한국 공휴일 (2025년 기준 - 정확한 날짜)
        holidays_2025 = [
            '2025-01-01',  # 신정
            '2025-01-28',  # 설날
            '2025-01-29',  # 설날
            '2025-01-30',  # 설날
            '2025-03-01',  # 삼일절
            '2025-05-05',  # 어린이날
            '2025-05-12',  # 부처님오신날
            '2025-06-06',  # 현충일
            '2025-08-15',  # 광복절
            '2025-10-05',  # 추석
            '2025-10-06',  # 추석
            '2025-10-07',  # 추석
            '2025-10-03',  # 개천절
            '2025-10-09',  # 한글날
            '2025-12-25',  # 크리스마스
        ]
        
        # 해당 월의 모든 날짜
        days_in_month = calendar.monthrange(year, month)[1]
        business_days = 0
        
        for day in range(1, days_in_month + 1):
            date = datetime(year, month, day)
            weekday = date.strftime('%a')  # 요일 (Mon, Tue, Wed, Thu, Fri, Sat, Sun)
            date_str = date.strftime('%Y-%m-%d')
            
            # 공휴일 체크
            if date_str in holidays_2025:
                continue
            
            # 일요일 격주 운영 체크 (월의 첫째 주와 셋째 주만 운영)
            if weekday == 'Sun':
                week_number = (day - 1) // 7 + 1
                if week_number not in [1, 3]:  # 첫째 주, 셋째 주만
                    continue
            
            business_days += 1
        
        return business_days
    
    def calculate_mission_target(self, year, month, adjustment_factor=1.0):
        """월간미션 목표 계산"""
        # 1. 이전 달 데이터 조회
        prev_month_data = self.get_previous_month_data(year, month)
        branch_weekday_data = self.get_branch_weekday_data(year, month)
        
        if not prev_month_data or 'data' not in prev_month_data:
            return {
                'error': f'{year-1 if month == 1 else year}년 {month-1 if month > 1 else 12}월 데이터가 없습니다.'
            }
        
        # 2. 요일별 평균 거리 계산
        weekday_averages = self.calculate_weekday_averages(prev_month_data)
        
        # 3. 해당 월의 영업일수 계산
        business_days = self.get_business_days(year, month)
        
        # 4. 예상 달성 km 계산
        total_expected_km = 0
        weekday_business_days = {}
        
        # 요일별 영업일수 계산
        days_in_month = calendar.monthrange(year, month)[1]
        for day in range(1, days_in_month + 1):
            date = datetime(year, month, day)
            weekday = date.strftime('%a')
            date_str = date.strftime('%Y-%m-%d')
            
            # 공휴일 체크
            holidays_2025 = [
                '2025-01-01', '2025-01-28', '2025-01-29', '2025-01-30',
                '2025-03-01', '2025-05-05', '2025-05-12', '2025-06-06',
                '2025-08-15', '2025-10-05', '2025-10-06', '2025-10-07',
                '2025-10-03', '2025-10-09', '2025-12-25'
            ]
            
            if date_str in holidays_2025:
                continue
            
            # 일요일 격주 운영 체크
            if weekday == 'Sun':
                week_number = (day - 1) // 7 + 1
                if week_number not in [1, 3]:
                    continue
            
            if weekday not in weekday_business_days:
                weekday_business_days[weekday] = 0
            weekday_business_days[weekday] += 1
        
        # 요일별 예상 거리 계산
        for weekday, days in weekday_business_days.items():
            if weekday in weekday_averages:
                total_expected_km += weekday_averages[weekday] * days
        
        # 5. 조정 팩터 적용
        adjusted_target = total_expected_km * adjustment_factor
        
        return {
            'year': year,
            'month': month,
            'previous_month_data': {
                'year': year-1 if month == 1 else year,
                'month': month-1 if month > 1 else 12,
                'total_records': len(prev_month_data['data']),
                'weekday_averages': weekday_averages
            },
            'branch_weekday_data': branch_weekday_data['data'] if branch_weekday_data and 'data' in branch_weekday_data else [],
            'business_days': business_days,
            'weekday_business_days': weekday_business_days,
            'expected_km': round(total_expected_km, 2),
            'adjusted_target': round(adjusted_target, 2),
            'adjustment_factor': adjustment_factor
        }

# Flask API 엔드포인트
calculator = MonthlyMissionCalculator()

@app.route('/')
def index():
    """메인 페이지"""
    return render_template('index.html')

@app.route('/api/calculate', methods=['POST'])
def calculate_mission():
    """월간미션 계산 API"""
    try:
        data = request.get_json()
        year = int(data.get('year'))
        month = int(data.get('month'))
        adjustment_factor = float(data.get('adjustment_factor', 1.0))
        
        if not calculator.connect():
            return jsonify({'error': '데이터베이스 연결 실패'}), 500
        
        try:
            result = calculator.calculate_mission_target(year, month, adjustment_factor)
            return jsonify(result)
        finally:
            calculator.disconnect()
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/previous-month-data', methods=['POST'])
def get_previous_month_data():
    """이전 달 데이터 조회 API"""
    try:
        data = request.get_json()
        year = int(data.get('year'))
        month = int(data.get('month'))
        
        if not calculator.connect():
            return jsonify({'error': '데이터베이스 연결 실패'}), 500
        
        try:
            result = calculator.get_previous_month_data(year, month)
            if result and 'data' in result:
                return jsonify({
                    'success': True,
                    'data': result['data'],
                    'columns': result['columns'],
                    'row_count': result['row_count']
                })
            else:
                return jsonify({'error': '데이터를 찾을 수 없습니다.'}), 404
        finally:
            calculator.disconnect()
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=False, host='0.0.0.0', port=port)
