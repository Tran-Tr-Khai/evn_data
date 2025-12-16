import os
import sys
import logging
import time
import random
import locale
from datetime import datetime, timedelta, date
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from curl_cffi import requests

sys.stdout.reconfigure(encoding='utf-8')
# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Configs ---
load_dotenv()

class Config:
    # Database
    SERVER = os.getenv('SERVER')
    USER = os.getenv('DB_USERNAME')
    PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DATABASE2')
    DRIVER = 'ODBC Driver 17 for SQL Server' 
    
    # CPC API
    DOMAIN = "https://cskh.cpc.vn"
    API_LOGIN = "https://cskh-api.cpc.vn/api/cskh/user/login"
    API_DATA = "https://cskh-api.cpc.vn/api/remote/dspm/bieudodongdien"
    
    # Customer Info
    USERNAME = os.getenv('CPC_USERNAME')
    PASSWORD_CPC = os.getenv('CPC_PASSWORD')
    CUST_CODE = os.getenv('CUST_CODE')
    CUST_POINT = os.getenv('CUST_POINT')

    @classmethod
    def get_db_uri(cls):
        return f"mssql+pyodbc://{cls.USER}:{cls.PASSWORD}@{cls.SERVER}/{cls.DB_NAME}?driver={cls.DRIVER.replace(' ', '+')}"

def get_session():
    # --- UPDATE 2: Giả lập Chrome 120 (Impersonate) ---
    # Điều này giúp vượt qua các tường lửa kiểm tra TLS Fingerprint
    session = requests.Session(impersonate="chrome120")
    
    session.headers.update({
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': Config.DOMAIN,
        'Referer': f"{Config.DOMAIN}/",
        # Không cần User-Agent thủ công vì impersonate đã tự lo
    })
    return session

def login(session):
    payload = {
        "username": Config.USERNAME,
        "password": Config.PASSWORD_CPC,
        "grant_type": "password",
        "scope": "CSKH",
        "ThongTinCaptcha": {"captcha": "undefined", "token": "undefined"}
    }
    try:
        logger.info(f"Đang đăng nhập user: {Config.USERNAME}...")
        resp = session.post(Config.API_LOGIN, json=payload, timeout=30)
        
        if resp.status_code == 200:
            token = resp.json().get('access_token')
            if token:
                session.headers.update({'Authorization': f'Bearer {token}'})
                logger.info("Login thành công.")
                return True
        
        logger.error(f"Login thất bại. Status: {resp.status_code} - Body: {resp.text}")
        return False
    except Exception as e:
        logger.exception(f"Exception khi login: {e}")
        return False

def fetch_data(session, date_str):
    params = {
        'customerPoint': Config.CUST_POINT,
        'customerCode': Config.CUST_CODE,
        'from': date_str,
        'to': date_str,
        'SkipCount': 0,
        'MaxResultCount': 1000
    }
    
    # --- UPDATE 3: Custom Retry Loop cho curl_cffi ---
    max_retries = 5
    for attempt in range(max_retries):
        try:
            logger.info(f"Đang tải dữ liệu ngày {date_str} (Lần {attempt + 1})...")
            resp = session.get(Config.API_DATA, params=params, timeout=30)
            
            if resp.status_code == 200:
                items = resp.json().get('soLieu', {}).get('items', [])
                logger.info(f"-> Tải về được {len(items)} dòng.")
                return items
            elif resp.status_code == 429:
                logger.warning("Bị giới hạn request (429). Nghỉ 10s...")
                time.sleep(10)
            elif resp.status_code == 400:
                # THÊM DÒNG NÀY ĐỂ ĐỌC LỖI
                logger.error(f"Lỗi 400 - Server trả về: {resp.text}") 
                time.sleep(5) # Nghỉ lâu hơn chút khi gặp lỗi này
            else:
                resp.raise_for_status()
                
        except Exception as e:
            logger.error(f"Lỗi fetch data lần {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1)) # Backoff: 2s, 4s...
            else:
                logger.error("Đã hết số lần thử lại.")
                
    return []

def safe_float(value):
    try:
        if value is None: return None
        return float(value)
    except (ValueError, TypeError):
        return None

def map_raw_to_clean(raw_items):
    clean_data = []
    MONTH_MAP = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',
                 'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
    
    for item in raw_items:
        raw_time = item.get('ngaygio') 
        dt_object = None
        
        # 1. Parse time
        if raw_time:
            try:
                dt_object = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                # Fallback logic
                try:
                    raw_time_gio = item.get('gio')
                    if raw_time_gio:
                        parts = raw_time_gio.split()
                        if len(parts) == 4:
                            day, month_str, year, time_str = parts
                            if month_str in MONTH_MAP:
                                new_str = f"{day} {MONTH_MAP[month_str]} {year} {time_str}"
                                dt_object = datetime.strptime(new_str, "%d %m %Y %H:%M")
                except:
                    pass

        if dt_object:
            row = {
                'ThoiGian':       dt_object,
                'DienAp_A':       safe_float(item.get('v_A')), 
                'DienAp_B':       safe_float(item.get('v_B')),
                'DienAp_C':       safe_float(item.get('v_C')),
                'DongDien_A':     safe_float(item.get('a_A')),
                'DongDien_B':     safe_float(item.get('a_B')),
                'DongDien_C':     safe_float(item.get('a_C')),
                'CosPhi_A':       safe_float(item.get('pF_A')),
                'CosPhi_B':       safe_float(item.get('pF_B')),
                'CosPhi_C':       safe_float(item.get('pF_C')),
                'TongCongSuat_P': safe_float(item.get('aP_T')),
                'ChiSo_DienNang': safe_float(item.get('importkwh')), 
                'CSDN_bt':        safe_float(item.get('impbt')), 
                'CSDN_cd':        safe_float(item.get('impcd')), 
                'CSDN_td':        safe_float(item.get('imptd'))
            }
            clean_data.append(row)
            
    return clean_data

def save_to_sqlserver_bulk(new_data, date_str, table_name="evncpc_tb"):
    if not new_data: return

    # Dùng engine global hoặc khởi tạo mới (khuyên dùng global nếu có)
    engine = create_engine(Config.get_db_uri())
    
    try:
        # Sử dụng engine.begin() để tự động quản lý Transaction (Auto-commit/Rollback)
        with engine.begin() as conn:
            
            # Câu lệnh SQL "Thông minh":
            # Thay vì INSERT VALUES, ta dùng INSERT SELECT ... WHERE NOT EXISTS
            # Nó sẽ kiểm tra từng dòng: Nếu ThoiGian chưa có thì mới Insert.
            insert_stmt = text(f"""
                INSERT INTO {table_name} 
                (ThoiGian, DienAp_A, DienAp_B, DienAp_C, DongDien_A, DongDien_B, DongDien_C, 
                 CosPhi_A, CosPhi_B, CosPhi_C, TongCongSuat_P, ChiSo_DienNang, CSDN_bt, CSDN_cd, CSDN_td)
                SELECT 
                    :ThoiGian, :DienAp_A, :DienAp_B, :DienAp_C, :DongDien_A, :DongDien_B, :DongDien_C, 
                    :CosPhi_A, :CosPhi_B, :CosPhi_C, :TongCongSuat_P, :ChiSo_DienNang, :CSDN_bt, :CSDN_cd, :CSDN_td
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} WHERE ThoiGian = :ThoiGian
                )
            """)

            # SQLAlchemy sẽ tự động chạy câu lệnh trên cho từng dòng trong list new_data
            result = conn.execute(insert_stmt, new_data)
            
            # rowcount sẽ trả về số dòng thực sự được Insert (không tính các dòng bị bỏ qua do trùng)
            logger.info(f"[SQL] Đã xử lý xong ngày {date_str}. Thêm mới: {result.rowcount}/{len(new_data)} dòng.")

    except Exception as e:
        logger.error(f"DB Error: {e}")
def get_latest_date_in_db():
    engine = create_engine(Config.get_db_uri())
    try:
        with engine.connect() as conn:
            query = text(f"SELECT MAX(ThoiGian) FROM evncpc_tb")
            result = conn.execute(query).fetchone()
            if result and result[0]:
                return result[0].date()
    except Exception as e:
        logger.error(f"Check Date Error: {e}")
    return None

def run_etl_transform(date_str):
    """
    Gọi Stored Procedure để clean và nội suy dữ liệu cho ngày date_str
    """
    engine = create_engine(Config.get_db_uri())
    
    # Tính toán tham số cho SP
    # Ví dụ: date_str = "2025-12-15"
    # FromDate = 2025-12-15 00:00:00
    # ToDate   = 2025-12-16 00:00:00
    try:
        current_dt = datetime.strptime(date_str, '%Y-%m-%d')
        next_dt = current_dt + timedelta(days=1)
        
        logger.info(f"[ETL] Đang chạy transform cho ngày: {date_str}...")
        
        with engine.begin() as conn: # Dùng begin để auto-commit
            # Gọi Stored Procedure
            query = text("EXEC sp_ETL_Clean_EVN_Data :FromDate, :ToDate")
            
            conn.execute(query, {
                "FromDate": current_dt, 
                "ToDate": next_dt
            })
            
        logger.info(f"[ETL] Hoàn tất transform ngày {date_str}.")
        
    except Exception as e:
        logger.error(f"[ETL] Lỗi khi chạy SP: {e}")

# def main():
#     if not Config.USERNAME or not Config.PASSWORD_CPC:
#         logger.critical("LỖI: Chưa cấu hình .env")
#         sys.exit(1)

#     session = get_session()
#     if not login(session):
#         return

#     latest_db_date = get_latest_date_in_db()

#     # Logic xác định ngày bắt đầu:
#     # Nếu DB có data, lùi lại 1 ngày để đảm bảo quét đủ (trường hợp data hôm qua chưa về hết)
#     target_date = date(2025, 12, 1)
#     if latest_db_date:
#         target_date = latest_db_date
#         logger.info(f"Tiếp tục quét từ ngày: {target_date}")
#     else:
#         logger.info(f"Quét mới từ đầu: {target_date}")

#     today_date = datetime.now().date()
#     current_process_date = target_date

#     while current_process_date <= today_date:
#         date_str = current_process_date.strftime('%Y-%m-%d')
#         logger.info(f"--- Processing: {date_str} ---")
    
#         raw_items = fetch_data(session, date_str)
#         if raw_items:
#             clean_items = map_raw_to_clean(raw_items)
#             save_to_sqlserver_bulk(clean_items, date_str)
#             run_etl_transform(date_str)
#         else:
#             logger.warning(f"No data for {date_str}")
        
#         current_process_date += timedelta(days=1)
    
#         # --- UPDATE 4: RANDOM SLEEP ĐỂ TRÁNH BLOCK ---
#         # Ngẫu nhiên nghỉ từ 2 đến 5 giây giữa các ngày
#         if current_process_date <= today_date:
#             sleep_time = random.uniform(2, 5)
#             logger.info(f"Nghỉ {sleep_time:.2f}s để tránh bị chặn...")
#             time.sleep(sleep_time)

#     logger.info("Done.")

# def save_to_sqlserver_safe(new_data, table_name="evncpc_tb"):
#     if not new_data: return

#     engine = create_engine(Config.get_db_uri())
    
#     # Câu lệnh Insert cơ bản
#     insert_stmt = text(f"""
#         INSERT INTO {table_name} 
#         (ThoiGian, DienAp_A, DienAp_B, DienAp_C, DongDien_A, DongDien_B, DongDien_C, 
#          CosPhi_A, CosPhi_B, CosPhi_C, TongCongSuat_P, ChiSo_DienNang, CSDN_bt, CSDN_cd, CSDN_td)
#         VALUES (:ThoiGian, :DienAp_A, :DienAp_B, :DienAp_C, :DongDien_A, :DongDien_B, :DongDien_C, 
#                 :CosPhi_A, :CosPhi_B, :CosPhi_C, :TongCongSuat_P, :ChiSo_DienNang, :CSDN_bt, :CSDN_cd, :CSDN_td)
#     """)

#     count_success = 0
#     with engine.connect() as conn:
#         for item in new_data:
#             try:
#                 # Insert từng dòng. Nếu dòng này trùng thì chỉ lỗi dòng này, không chết cả mẻ.
#                 conn.execute(insert_stmt, item)
#                 conn.commit()
#                 count_success += 1
#             except Exception as e:
#                 # Nếu lỗi là do trùng khóa (IntegrityError/Duplicate key), ta bỏ qua êm đẹp
#                 if "2627" in str(e) or "UNIQUE KEY" in str(e): 
#                     # logger.warning(f"Bỏ qua bản ghi trùng: {item['ThoiGian']}")
#                     pass
#                 else:
#                     logger.error(f"Lỗi insert dòng {item['ThoiGian']}: {e}")
                    
#     logger.info(f"[SQL] Đã lưu thành công {count_success}/{len(new_data)} dòng.")

# def main_fix_missing():
#     # 1. Cấu hình
#     if not Config.USERNAME or not Config.PASSWORD_CPC:
#         logger.critical("LỖI: Chưa cấu hình .env")
#         sys.exit(1)

#     # 2. Login
#     session = get_session()
#     if not login(session):
#         return

#     # 3. DANH SÁCH NGÀY CẦN CHẠY LẠI (Điền thủ công các ngày bị thiếu vào đây)
#     # Format: YYYY-MM-DD
#     MISSING_DATES = [
#         "2025-12-10",
#         "2025-12-11"
#         # "2025-12-07", # Thêm ngày nào thiếu thì điền vào đây
#     ]

#     logger.info(f"Bắt đầu quy trình vá lỗi cho {len(MISSING_DATES)} ngày...")

#     for date_str in MISSING_DATES:
#         logger.info(f"--- Đang vá dữ liệu ngày: {date_str} ---")
        
#         # Tăng số lần thử lại lên 10 lần cho chắc ăn
#         # (Bạn có thể sửa trực tiếp số lần loop trong hàm fetch_data nếu cần)
#         raw_items = fetch_data(session, date_str) 
        
#         if raw_items:
#             clean_items = map_raw_to_clean(raw_items)
#             # Dùng hàm save mới (save_to_sqlserver_safe) thay vì hàm bulk cũ
#             save_to_sqlserver_safe(clean_items)
#         else:
#             logger.error(f"Vẫn không lấy được data ngày {date_str} sau nỗ lực retry.")
        
#         # Nghỉ ngơi giữa các ngày để server không khóa IP
#         logger.info("Nghỉ 5s...")
#         time.sleep(5)

#     logger.info("Hoàn tất quy trình vá lỗi.")

def main():
    logger.info(">>> START JOB <<<")
    # check config
    if not Config.USERNAME or not Config.PASSWORD_CPC:
        logger.critical("LỖI: Chưa cấu hình .env")
        sys.exit(1)

    # login
    session = get_session()
    if not login(session):
        return

    # Ngày quét data 
    latest_db_date = get_latest_date_in_db()
    today_date = datetime.now().date()
    
    if latest_db_date:
        # Nếu đã có data, lùi lại 1 ngày so với ngày mới nhất trong DB
        # Để đảm bảo cập nhật lại các chỉ số chốt ngày (nếu hôm qua chưa chốt xong)
        target_date = latest_db_date - timedelta(days=1)
        logger.info(f"DB đang có data tới: {latest_db_date}. Sẽ quét lại từ: {target_date}")
    else:
        target_date = date(2025, 12, 1) 
        logger.info(f"DB chưa có data. Quét mới từ đầu: {target_date}")

    current_process_date = target_date

    while current_process_date <= today_date:
        date_str = current_process_date.strftime('%Y-%m-%d')
        logger.info(f"--- Processing: {date_str} ---")
    
        raw_items = fetch_data(session, date_str)
        
        if raw_items:
            # load raw
            clean_items = map_raw_to_clean(raw_items)
            save_to_sqlserver_bulk(clean_items, date_str)
            
            # etl 
            run_etl_transform(date_str)
        else:
            logger.warning(f"Không có dữ liệu cho ngày {date_str}")
        
        # Tăng ngày
        current_process_date += timedelta(days=1)
    
        # Nghỉ ngẫu nhiên để tránh Block (chỉ nghỉ nếu chưa phải ngày cuối)
        if current_process_date <= today_date:
            sleep_time = random.uniform(2, 5)
            logger.info(f"Nghỉ {sleep_time:.2f}s...")
            time.sleep(sleep_time)

    logger.info(">>> JOB FINISHED <<<")

if __name__ == '__main__':
    # Kiểm tra cấu hình
    if not Config.USERNAME or not Config.PASSWORD_CPC:
        logger.critical("LỖI: Chưa cấu hình .env")
        sys.exit(1)
        
    main()
