-- Xóa bảng cũ nếu tồn tại để tạo lại
IF OBJECT_ID('evncpc_tb', 'U') IS NOT NULL DROP TABLE evncpc_tb;

CREATE TABLE evncpc_tb (
    id BIGINT IDENTITY(1,1) NOT NULL,
    ThoiGian DATETIME NOT NULL,
    DienAp_A FLOAT, DienAp_B FLOAT, DienAp_C FLOAT,
    DongDien_A FLOAT, DongDien_B FLOAT, DongDien_C FLOAT,
    CosPhi_A FLOAT, CosPhi_B FLOAT, CosPhi_C FLOAT,
    TongCongSuat_P FLOAT,
    ChiSo_DienNang DECIMAL(18, 3), 
    CSDN_bt DECIMAL(18, 3),
    CSDN_cd DECIMAL(18, 3),
    CSDN_td DECIMAL(18, 3),
    created_at DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_evncpc_tb PRIMARY KEY NONCLUSTERED (id),
    CONSTRAINT UQ_ThoiGian_EVN UNIQUE (ThoiGian)
);
CREATE CLUSTERED INDEX [CIX_evncpc_ThoiGian] ON evncpc_tb(ThoiGian);

-- Xóa bảng cũ nếu tồn tại (Cẩn thận khi chạy lệnh này trên production)
IF OBJECT_ID('evncpc_final', 'U') IS NOT NULL DROP TABLE evncpc_final;

CREATE TABLE evncpc_final (
    ThoiGian DATETIME NOT NULL,
    
    -- Các thông số điện
    DienAp_A FLOAT, DienAp_B FLOAT, DienAp_C FLOAT,
    DongDien_A FLOAT, DongDien_B FLOAT, DongDien_C FLOAT,
    CosPhi_A FLOAT, CosPhi_B FLOAT, CosPhi_C FLOAT,
    TongCongSuat_P FLOAT,
    
    -- Các chỉ số điện năng 
    ChiSo_DienNang DECIMAL(18, 3), 
    CSDN_bt DECIMAL(18, 3), -- Bình thường
    CSDN_cd DECIMAL(18, 3), -- Cao điểm
    CSDN_td DECIMAL(18, 3), -- Thấp điểm
    
    -- Cột quản lý
    Is_Interpolated BIT DEFAULT 0, -- 0: Thật, 1: Nội suy
    created_at DATETIME DEFAULT GETDATE(),

    CONSTRAINT PK_evncpc_final PRIMARY KEY CLUSTERED (ThoiGian)
);


--------------- transform data ---------------
CREATE OR ALTER PROCEDURE sp_ETL_Clean_EVN_Data
    @FromDate DATETIME,
    @ToDate DATETIME
AS
BEGIN
    SET NOCOUNT ON;

    -- Xóa dữ liệu cũ trong khoảng thời gian này (Idempotency)
    -- Để đảm bảo nếu chạy lại Job này 2 lần thì data không bị duplicate
    DELETE FROM evncpc_final 
    WHERE ThoiGian >= @FromDate AND ThoiGian < @ToDate;

    
    -- Tạo khung thời gian chuẩn (48 slot/ngày)
    WITH TimeSlots AS (
        SELECT @FromDate AS Slot
        UNION ALL
        SELECT DATEADD(MINUTE, 30, Slot)
        FROM TimeSlots
        WHERE Slot < DATEADD(MINUTE, -30, @ToDate)
    ),
    
    -- Lấy dữ liệu thô (Đã lọc trùng khung giờ)
    RawUnique AS (
        SELECT 
            *,
            TimeBucket = DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, ThoiGian) / 30) * 30, 0),
            Rn = ROW_NUMBER() OVER (
                    PARTITION BY DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, ThoiGian) / 30) * 30, 0) 
                    ORDER BY ThoiGian ASC
                 )
        FROM evncpc_tb
        WHERE ThoiGian >= @FromDate AND ThoiGian < @ToDate
    )
    
    -- 3. INSERT và NỘI SUY (Forward Fill)
    INSERT INTO evncpc_final (
        ThoiGian, 
        DienAp_A, DienAp_B, DienAp_C, 
        DongDien_A, DongDien_B, DongDien_C, 
        CosPhi_A, CosPhi_B, CosPhi_C, 
        TongCongSuat_P, 
        ChiSo_DienNang, CSDN_bt, CSDN_cd, CSDN_td, -- Đã thêm các cột mới
        Is_Interpolated
    )
    SELECT 
        T.Slot,
        -- Kỹ thuật COALESCE: Ưu tiên data thật (R), nếu NULL thì lấy data quá khứ (LastKnown)
        COALESCE(R.DienAp_A, LastKnown.DienAp_A),
        COALESCE(R.DienAp_B, LastKnown.DienAp_B),
        COALESCE(R.DienAp_C, LastKnown.DienAp_C),
        
        COALESCE(R.DongDien_A, LastKnown.DongDien_A),
        COALESCE(R.DongDien_B, LastKnown.DongDien_B),
        COALESCE(R.DongDien_C, LastKnown.DongDien_C),
        
        COALESCE(R.CosPhi_A, LastKnown.CosPhi_A),
        COALESCE(R.CosPhi_B, LastKnown.CosPhi_B),
        COALESCE(R.CosPhi_C, LastKnown.CosPhi_C),
        
        COALESCE(R.TongCongSuat_P, LastKnown.TongCongSuat_P),
        
        -- Các chỉ số điện năng
        COALESCE(R.ChiSo_DienNang, LastKnown.ChiSo_DienNang),
        COALESCE(R.CSDN_bt, LastKnown.CSDN_bt),
        COALESCE(R.CSDN_cd, LastKnown.CSDN_cd),
        COALESCE(R.CSDN_td, LastKnown.CSDN_td),

        -- Cờ đánh dấu: Nếu không tìm thấy ID trong bảng gốc tại giờ đó -> Là data nội suy
        CASE WHEN R.id IS NULL THEN 1 ELSE 0 END 

    FROM TimeSlots T
    LEFT JOIN RawUnique R ON T.Slot = R.TimeBucket AND R.Rn = 1
    -- Tìm giá trị gần nhất TRƯỚC ĐÓ (Forward Fill)
    OUTER APPLY (
        SELECT TOP 1 *
        FROM evncpc_tb OldData 
        WHERE OldData.ThoiGian < T.Slot 
        ORDER BY OldData.ThoiGian DESC
    ) LastKnown
    OPTION (MAXRECURSION 0);
END;


-- Tự động lấy ngày hôm qua
DECLARE @Yesterday DATE = CAST(GETDATE() - 1 AS DATE);
DECLARE @Today DATE = CAST(GETDATE() AS DATE);

EXEC sp_ETL_Clean_EVN_Data @Yesterday, @Today;