#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from db_connector import PostgreSQLConnector
from datetime import datetime
import json
import os
import webbrowser

class DatabaseQueryTool:
    def __init__(self):
        self.db = PostgreSQLConnector()
        self.saved_queries = {}
        self.load_saved_queries()
    
    def load_saved_queries(self):
        """저장된 쿼리들을 로드"""
        try:
            if os.path.exists('saved_queries.json'):
                with open('saved_queries.json', 'r', encoding='utf-8') as f:
                    self.saved_queries = json.load(f)
        except:
            self.saved_queries = {}
    
    def save_queries(self):
        """쿼리들을 파일에 저장"""
        try:
            with open('saved_queries.json', 'w', encoding='utf-8') as f:
                json.dump(self.saved_queries, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"❌ 쿼리 저장 실패: {e}")
    
    def connect(self):
        """데이터베이스 연결"""
        return self.db.connect()
    
    def disconnect(self):
        """데이터베이스 연결 해제"""
        self.db.disconnect()
    
    def get_table_list(self):
        """데이터베이스의 모든 테이블 목록 조회"""
        print("🔍 데이터베이스의 테이블 목록을 조회합니다...")
        result = self.db.get_table_list()
        
        if result and 'data' in result:
            print(f"\n📋 총 {result['row_count']}개의 테이블:")
            print("-" * 60)
            print(f"{'테이블명':40} | {'타입'}")
            print("-" * 60)
            
            for table_name, table_type in result['data']:
                print(f"{table_name:40} | {table_type}")
        else:
            print("❌ 테이블 목록을 가져올 수 없습니다.")
    
    def get_table_structure(self, table_name=None):
        """테이블 구조 확인"""
        if not table_name:
            table_name = input("테이블명을 입력하세요: ").strip()
        
        if not table_name:
            print("❌ 테이블명을 입력해주세요.")
            return
        
        print(f"🔍 '{table_name}' 테이블 구조를 확인합니다...")
        result = self.db.get_table_info(table_name)
        
        if result and 'data' in result:
            print(f"\n📋 '{table_name}' 테이블 구조:")
            print("-" * 80)
            print(f"{'컬럼명':25} | {'타입':20} | {'NULL허용':10} | {'기본값'}")
            print("-" * 80)
            
            for col_name, data_type, is_nullable, default_val in result['data']:
                default_str = str(default_val) if default_val else "없음"
                print(f"{col_name:25} | {data_type:20} | {is_nullable:10} | {default_str}")
        else:
            print(f"❌ 테이블 '{table_name}'을 찾을 수 없습니다.")
    
    def execute_custom_query(self):
        """사용자 정의 쿼리 실행"""
        print("\n📝 SQL 쿼리를 입력하세요 (종료하려면 'exit' 입력):")
        query = input("SQL> ")
        
        if query.lower().strip() == 'exit':
            return
        
        if not query.strip():
            print("❌ 쿼리를 입력해주세요.")
            return
        
        result = self.db.execute_query(query)
        
        if result and 'columns' in result:
            print(f"\n📊 쿼리 결과: {result['row_count']}개 행")
            print("-" * 120)
            
            # 헤더 출력
            headers = result['columns']
            print(" | ".join(f"{header:15}" for header in headers))
            print("-" * 120)
            
            # 데이터 출력 (최대 20행)
            for i, row in enumerate(result['data'][:20]):
                print(" | ".join(f"{str(val):15}" for val in row))
            
            if len(result['data']) > 20:
                print(f"... 그리고 {len(result['data']) - 20}개 행 더")
            
            return result
        elif result and 'message' in result:
            print(f"✅ {result['message']}")
            if 'affected_rows' in result:
                print(f"영향받은 행 수: {result['affected_rows']}")
        else:
            print("❌ 쿼리 실행에 실패했습니다.")
            return None
    
    def generate_appscript_code(self, data, spreadsheet_name=None):
        """Google Apps Script 코드 생성"""
        if not data or 'columns' not in data:
            print("❌ 생성할 데이터가 없습니다.")
            return None
        
        if not spreadsheet_name:
            spreadsheet_name = f"쿼리결과_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        headers = data['columns']
        rows = data['data']
        
        # 날짜 객체를 문자열로 변환
        def convert_to_serializable(obj):
            if hasattr(obj, 'isoformat'):  # datetime, date 객체
                return obj.isoformat()
            elif obj is None:
                return None
            else:
                return str(obj)
        
        # 모든 데이터를 직렬화 가능한 형태로 변환
        serializable_rows = []
        for row in rows:
            serializable_row = [convert_to_serializable(cell) for cell in row]
            serializable_rows.append(serializable_row)
        
        headers_js = json.dumps(headers, ensure_ascii=False)
        data_js = json.dumps(serializable_rows, ensure_ascii=False)
        
        appscript_code = f"""
function createQueryResultSheet() {{
  // 스프레드시트 생성
  const spreadsheet = SpreadsheetApp.create('{spreadsheet_name}');
  const sheet = spreadsheet.getActiveSheet();
  
  // 헤더 설정
  const headers = {headers_js};
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  
  // 헤더 스타일링
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setBackground('#366092');
  headerRange.setFontColor('#FFFFFF');
  headerRange.setFontWeight('bold');
  
  // 데이터 입력
  const data = {data_js};
  if (data.length > 0) {{
    sheet.getRange(2, 1, data.length, data[0].length).setValues(data);
  }}
  
  // 열 너비 자동 조정
  sheet.autoResizeColumns(1, headers.length);
  
  // 스프레드시트 URL 출력
  console.log('스프레드시트 URL: ' + spreadsheet.getUrl());
  
  return spreadsheet.getUrl();
}}

// 실행 함수
function main() {{
  return createQueryResultSheet();
}}
"""
        
        return appscript_code, spreadsheet_name
    
    def save_appscript_file(self, appscript_code, filename=None):
        """Apps Script 코드를 파일로 저장"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_result_{timestamp}.js"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(appscript_code)
            
            print(f"✅ Apps Script 코드가 저장되었습니다: {filename}")
            return filename
        except Exception as e:
            print(f"❌ 파일 저장 실패: {e}")
            return None
    
    def open_appscript_editor(self):
        """Google Apps Script 에디터 열기"""
        try:
            webbrowser.open('https://script.google.com/')
            print("🌐 Google Apps Script 에디터가 열렸습니다.")
            print("📋 저장된 .js 파일의 내용을 복사해서 붙여넣으세요.")
        except Exception as e:
            print(f"❌ 브라우저 열기 실패: {e}")
    
    def export_query_to_appscript(self, query, spreadsheet_name=None):
        """쿼리 결과를 Apps Script로 내보내기"""
        print("📊 쿼리 결과를 Apps Script로 내보내기...")
        
        result = self.db.execute_query(query)
        
        if not result or 'data' not in result:
            print("❌ 내보낼 데이터가 없습니다.")
            return None
        
        appscript_code, sheet_name = self.generate_appscript_code(result, spreadsheet_name)
        
        if not appscript_code:
            return None
        
        filename = self.save_appscript_file(appscript_code)
        
        if filename:
            print(f"\n📋 다음 단계를 따라하세요:")
            print(f"1. 저장된 파일 '{filename}'을 열어서 내용을 복사")
            print(f"2. https://script.google.com/ 에서 새 프로젝트 생성")
            print(f"3. 코드를 붙여넣고 'main' 함수 실행")
            print(f"4. '{sheet_name}' 스프레드시트가 자동 생성됩니다!")
            
            open_editor = input("\nGoogle Apps Script 에디터를 지금 열까요? (y/n): ").strip().lower()
            if open_editor == 'y':
                self.open_appscript_editor()
            
            return filename
        
        return None
    
    def save_to_excel(self, query, filename=None):
        """쿼리 결과를 Excel 파일로 저장"""
        print("📊 쿼리 결과를 Excel로 저장합니다...")
        
        result = self.db.execute_query(query)
        
        if result and 'data' in result:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"query_result_{timestamp}.xlsx"
            
            success = self.db.save_to_excel(result, filename)
            
            if success:
                print(f"✅ 쿼리 결과가 저장되었습니다: {filename}")
                return filename
            else:
                return None
        else:
            print("❌ 저장할 데이터가 없습니다.")
            return None
    
    def save_query(self, name, query):
        """쿼리 저장"""
        self.saved_queries[name] = query
        self.save_queries()
        print(f"✅ 쿼리 '{name}'이 저장되었습니다.")
    
    def list_saved_queries(self):
        """저장된 쿼리 목록"""
        if not self.saved_queries:
            print("📝 저장된 쿼리가 없습니다.")
            return
        
        print("\n📋 저장된 쿼리 목록:")
        print("-" * 50)
        for i, (name, query) in enumerate(self.saved_queries.items(), 1):
            print(f"{i}. {name}")
            print(f"   {query[:60]}{'...' if len(query) > 60 else ''}")
    
    def run_saved_query(self, name):
        """저장된 쿼리 실행"""
        if name not in self.saved_queries:
            print(f"❌ 쿼리 '{name}'을 찾을 수 없습니다.")
            return None
        
        query = self.saved_queries[name]
        print(f"\n🔍 실행 중인 쿼리: {name}")
        return self.db.execute_query(query)
    
    def get_user_membership_info(self, user_name=None, phone_number=None):
        """사용자 이름과 폰번호로 멤버십 정보 조회"""
        print("\n👤 사용자 멤버십 정보 조회")
        print("=" * 40)
        
        if not user_name:
            user_name = input("사용자 이름을 입력하세요: ").strip()
        if not phone_number:
            phone_number = input("폰번호를 입력하세요: ").strip()
        
        if not user_name or not phone_number:
            print("❌ 사용자 이름과 폰번호를 모두 입력해주세요.")
            return None
        
        print(f"\n🔍 '{user_name}' (폰번호: {phone_number}) 사용자의 멤버십 현황을 조회합니다...")
        
        # 사용자 멤버십 현황 조회 쿼리 (간단한 정보만)
        query = f"""
        SELECT 
            u.name as 사용자이름,
            u.phone_number as 폰번호,
            bp_place.name as 이용지점,
            bm.title as 멤버십명,
            bm.begin_date as 시작일,
            bm.end_date as 종료일,
            CASE 
                WHEN bm.is_active = true THEN '활성'
                ELSE '비활성'
            END as 상태
        FROM user_user u
        LEFT JOIN b_class_bpass bp ON u.id = bp.user_id
        LEFT JOIN b_class_bmembership bm ON bp.id = bm.b_pass_id
        LEFT JOIN b_class_bplace bp_place ON bp.b_place_id = bp_place.id
        WHERE u.name ILIKE '%{user_name}%' 
        AND u.phone_number ILIKE '%{phone_number}%'
        ORDER BY bm.begin_date DESC NULLS LAST;
        """
        
        result = self.db.execute_query(query)
        
        if result and 'data' in result and result['data']:
            print(f"\n📊 조회 결과: {result['row_count']}개의 멤버십")
            print("-" * 100)
            
            # 헤더 출력
            headers = result['columns']
            print(" | ".join(f"{header:15}" for header in headers))
            print("-" * 100)
            
            # 데이터 출력
            for i, row in enumerate(result['data']):
                print(" | ".join(f"{str(val):15}" for val in row))
            
            # Apps Script로 내보내기 옵션 제공
            export_choice = input(f"\n📋 이 결과를 Google Sheets로 내보내시겠습니까? (y/n): ").strip().lower()
            if export_choice == 'y':
                spreadsheet_name = input("스프레드시트 이름 (엔터시 자동생성): ").strip()
                if not spreadsheet_name:
                    spreadsheet_name = f"{user_name}_멤버십현황_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                filename = self.export_query_to_appscript(query, spreadsheet_name)
                if filename:
                    print(f"✅ Apps Script 파일이 생성되었습니다: {filename}")
            
            return result
        else:
            print("❌ 해당 사용자의 멤버십 정보를 찾을 수 없습니다.")
            return None

def main():
    print("🗄️ PostgreSQL 데이터베이스 쿼리 도구")
    print("=" * 50)
    
    tool = DatabaseQueryTool()
    
    if not tool.connect():
        return
    
    try:
        while True:
            print("\n📋 사용 가능한 옵션:")
            print("1. 테이블 목록 보기")
            print("2. 테이블 구조 확인")
            print("3. 사용자 정의 쿼리 실행")
            print("4. 쿼리 결과를 Apps Script로 내보내기")
            print("5. 쿼리 결과를 Excel로 저장")
            print("6. 저장된 쿼리 목록 보기")
            print("7. 새 쿼리 저장")
            print("8. 저장된 쿼리 실행")
            print("9. 사용자 멤버십 현황 조회")
            print("10. 종료")
            
            choice = input("\n선택하세요 (1-10): ").strip()
            
            if choice == '1':
                tool.get_table_list()
            
            elif choice == '2':
                tool.get_table_structure()
            
            elif choice == '3':
                tool.execute_custom_query()
            
            elif choice == '4':
                print("Apps Script로 내보낼 쿼리를 입력하세요:")
                query = input("SQL> ").strip()
                
                if query:
                    spreadsheet_name = input("스프레드시트 이름 (엔터시 자동생성): ").strip()
                    if not spreadsheet_name:
                        spreadsheet_name = None
                    
                    filename = tool.export_query_to_appscript(query, spreadsheet_name)
                    if filename:
                        print(f"✅ Apps Script 파일이 생성되었습니다: {filename}")
            
            elif choice == '5':
                print("Excel로 저장할 쿼리를 입력하세요:")
                query = input("SQL> ").strip()
                
                if query:
                    filename = input("파일명 (엔터시 자동생성): ").strip()
                    if not filename:
                        filename = None
                    
                    tool.save_to_excel(query, filename)
            
            elif choice == '6':
                tool.list_saved_queries()
            
            elif choice == '7':
                name = input("쿼리 이름을 입력하세요: ").strip()
                query = input("SQL 쿼리를 입력하세요: ").strip()
                if name and query:
                    tool.save_query(name, query)
            
            elif choice == '8':
                tool.list_saved_queries()
                name = input("실행할 쿼리 이름을 입력하세요: ").strip()
                result = tool.run_saved_query(name)
                if result and 'columns' in result:
                    print(f"\n📊 결과: {result['row_count']}개 행")
                    for i, row in enumerate(result['data'][:5]):
                        print(" | ".join(str(val) for val in row))
                    if len(result['data']) > 5:
                        print(f"... 그리고 {len(result['data']) - 5}개 행 더")
            
            elif choice == '9':
                tool.get_user_membership_info()
            
            elif choice == '10':
                print("👋 프로그램을 종료합니다.")
                break
            
            else:
                print("❌ 잘못된 선택입니다. 1-10 중에서 선택하세요.")
    
    finally:
        tool.disconnect()

if __name__ == "__main__":
    main()

