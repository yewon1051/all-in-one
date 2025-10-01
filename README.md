# 🗄️ PostgreSQL 데이터베이스 쿼리 도구

PostgreSQL 데이터베이스에서 자유롭게 쿼리를 실행하고 결과를 Google Apps Script나 Excel로 내보내는 범용 도구입니다.

## 🚀 설치 방법

### 1. 필요한 패키지 설치
```bash
pip install -r requirements.txt
```

### 2. Google Apps Script 사용법
Google Sheets 기능을 사용하려면:

1. https://script.google.com/ 에서 새 프로젝트 생성
2. 생성된 .js 파일의 내용을 복사해서 붙여넣기
3. 'main' 함수 실행
4. 자동으로 Google Sheets가 생성됩니다!

## 📋 사용 가능한 도구

### `db_query_tool.py` - 메인 도구
PostgreSQL 데이터베이스 쿼리 실행과 Google Apps Script 내보내기를 통합한 범용 도구

```bash
python3 db_query_tool.py
```

**주요 기능:**
- ✅ 데이터베이스 테이블 목록 조회
- ✅ 테이블 구조 확인
- ✅ 자유로운 SQL 쿼리 실행
- ✅ Google Apps Script 코드 생성 (Google Sheets 자동 생성)
- ✅ Excel 파일로 저장
- ✅ 쿼리 저장 및 재사용
- ✅ 쿼리 결과 화면 출력

## 📊 사용 예시

### 테이블 목록 확인
```
옵션 1 선택 → 모든 테이블 목록 조회
```

### 테이블 구조 확인
```
옵션 2 선택 → 테이블명 입력 → 컬럼 정보 확인
```

### 자유로운 쿼리 실행
```
옵션 3 선택 → SQL 쿼리 입력 → 결과 확인
```

### Google Sheets로 내보내기
1. 옵션 4 선택
2. SQL 쿼리 입력
3. 스프레드시트 이름 입력 (선택사항)
4. 자동으로 Google Sheets에 결과 저장

### 저장된 쿼리 사용
자주 사용하는 쿼리를 이름으로 저장하고 재사용할 수 있습니다.

## 🔍 주요 쿼리 예시

### 1. 테이블 목록 조회
```sql
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;
```

### 2. 특정 테이블의 데이터 조회
```sql
SELECT * FROM your_table_name 
WHERE created_at >= NOW() - INTERVAL '7 days'
ORDER BY created_at DESC
LIMIT 100;
```

### 3. 통계 쿼리
```sql
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_count,
    SUM(amount) as total_amount
FROM your_table_name 
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY date DESC;
```

## 📁 파일 구조

```
dbdbd/
├── requirements.txt              # 필요한 패키지 목록
├── db_connector.py              # PostgreSQL 연결 클래스
├── db_query_tool.py             # 메인 통합 도구 (범용 쿼리)
├── saved_queries.json           # 저장된 쿼리 (자동 생성)
└── README.md                    # 이 파일
```

## ⚠️ 주의사항

1. **데이터베이스 연결 정보**: `db_connector.py`에 하드코딩되어 있습니다. 보안을 위해 환경변수 사용을 권장합니다.

2. **Google Apps Script**: Google 계정만 있으면 바로 사용 가능합니다. 별도 설정 불필요!

3. **쿼리 최적화**: 대용량 데이터 조회 시 LIMIT을 사용하여 성능을 고려하세요.

## 🛠️ 문제 해결

### 데이터베이스 연결 실패
- 네트워크 연결 확인
- 데이터베이스 서버 상태 확인
- 연결 정보 재확인

### Google Apps Script 실행 실패
- Google 계정 로그인 확인
- Apps Script 에디터에서 권한 허용
- 생성된 .js 파일 내용이 올바른지 확인

### 쿼리 실행 실패
- 테이블명과 컬럼명 확인
- SQL 문법 검증
- 데이터베이스 권한 확인
