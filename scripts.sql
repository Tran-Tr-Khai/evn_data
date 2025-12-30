-- Xóa bảng cũ nếu tồn tại để tạo lại
IF OBJECT_ID('evncpc_tb_2', 'U') IS NOT NULL DROP TABLE evncpc_tb_2;

CREATE TABLE evncpc_tb_2(
    id BIGINT IDENTITY(1,1) PRIMARY KEY, 

    -- Voltage
    voltage_AB FLOAT, voltage_BC FLOAT, voltage_CA FLOAT, voltage_LL_avg FLOAT,
    voltage_AN FLOAT, voltage_BN FLOAT, voltage_CN FLOAT, voltage_LN_avg FLOAT,
    
    -- Current
    current_A FLOAT, current_B FLOAT, current_C FLOAT, current_avg FLOAT,
    
    -- Power
    power_A FLOAT, power_B FLOAT, power_C FLOAT, power_total FLOAT,
    
    -- Energy
    power_factor FLOAT, 
    energy_kWh FLOAT,
    energy_bt FLOAT,
    energy_cd FLOAT,
    energy_td FLOAT,
    
    timestamp DATETIME NOT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    CONSTRAINT UQ_timestamp_EVN_2 UNIQUE (timestamp)
);
CREATE CLUSTERED INDEX [CIX_evncpc_tb_2_timestamp] ON evncpc_tb_2(timestamp);

-- Xóa bảng cũ nếu tồn tại
IF OBJECT_ID('evncpc_final_2', 'U') IS NOT NULL DROP TABLE evncpc_final_2;

CREATE TABLE evncpc_final_2 (
    timestamp DATETIME NOT NULL,
    
    -- Voltage
    voltage_AB FLOAT, voltage_BC FLOAT, voltage_CA FLOAT, voltage_LL_avg FLOAT,
    voltage_AN FLOAT, voltage_BN FLOAT, voltage_CN FLOAT, voltage_LN_avg FLOAT,
    
    -- Current
    current_A FLOAT, current_B FLOAT, current_C FLOAT, current_avg FLOAT,
    
    -- Power
    power_A FLOAT, power_B FLOAT, power_C FLOAT, power_total FLOAT,
    
    -- Energy
    power_factor FLOAT, 
    energy_kWh FLOAT,
    energy_bt FLOAT,
    energy_cd FLOAT,
    energy_td FLOAT,
    
    -- Cột quản lý
    Is_Interpolated BIT DEFAULT 0, -- 0: Thật, 1: Nội suy
    created_at DATETIME DEFAULT GETDATE(),

    CONSTRAINT PK_evncpc_final_2 PRIMARY KEY CLUSTERED (timestamp)
);


--------------- transform data ---------------
CREATE OR ALTER PROCEDURE sp_ETL_Clean_EVN_Data
    @FromDate DATETIME,
    @ToDate DATETIME
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Kiểm tra mức độ lồng (Nesting Level) để tránh đệ quy vô hạn
    IF TRIGGER_NESTLEVEL() > 10
    BEGIN
        PRINT 'Cảnh báo: Mức độ lồng quá cao. Dừng thực thi.';
        RETURN;
    END

    -- 2. Xóa dữ liệu cũ trong khoảng thời gian này (Idempotency)
    DELETE FROM evncpc_final_2 
    WHERE timestamp >= @FromDate AND timestamp < @ToDate;

    -- 3. Xác định mốc thời gian cuối cùng có dữ liệu thực tế trong khoảng này
    -- Để tránh nội suy "vượt mức" vào tương lai khi chưa có data.
    DECLARE @MaxDataTime DATETIME;
    SELECT @MaxDataTime = MAX(timestamp) 
    FROM evncpc_tb_2 
    WHERE timestamp >= @FromDate AND timestamp < @ToDate;

    -- Nếu không có dữ liệu nào trong khoảng này, thoát luôn
    IF @MaxDataTime IS NULL RETURN;

    -- 4. Tạo bảng tạm chứa các mốc thời gian 30p (Tránh dùng Recursive CTE nếu môi trường bị giới hạn)
    DECLARE @TimeSlots TABLE (Slot DATETIME PRIMARY KEY);
    DECLARE @CurrentSlot DATETIME = @FromDate;
    
    -- Chỉ tạo các slot cho đến mốc thời gian cuối cùng có dữ liệu
    WHILE @CurrentSlot <= @MaxDataTime
    BEGIN
        INSERT INTO @TimeSlots (Slot) VALUES (@CurrentSlot);
        SET @CurrentSlot = DATEADD(MINUTE, 30, @CurrentSlot);
    END

    -- 5. Lấy dữ liệu thô duy nhất cho mỗi khung giờ
    -- (Dùng bảng tạm để tăng tốc độ join)
    DECLARE @RawUnique TABLE (
        TimeBucket DATETIME PRIMARY KEY,
        voltage_AB FLOAT, voltage_BC FLOAT, voltage_CA FLOAT, voltage_LL_avg FLOAT,
        voltage_AN FLOAT, voltage_BN FLOAT, voltage_CN FLOAT, voltage_LN_avg FLOAT,
        current_A FLOAT, current_B FLOAT, current_C FLOAT, current_avg FLOAT,
        power_A FLOAT, power_B FLOAT, power_C FLOAT, power_total FLOAT,
        power_factor FLOAT, energy_kWh FLOAT, energy_bt FLOAT, energy_cd FLOAT, energy_td FLOAT,
        HasData BIT
    );

    INSERT INTO @RawUnique
    SELECT 
        TimeBucket,
        voltage_AB, voltage_BC, voltage_CA, voltage_LL_avg, 
        voltage_AN, voltage_BN, voltage_CN, voltage_LN_avg, 
        current_A, current_B, current_C, current_avg, 
        power_A, power_B, power_C, power_total, 
        power_factor, energy_kWh, energy_bt, energy_cd, energy_td,
        1
    FROM (
        SELECT 
            *,
            TimeBucket = DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, timestamp) / 30) * 30, 0),
            Rn = ROW_NUMBER() OVER (
                    PARTITION BY DATEADD(MINUTE, (DATEDIFF(MINUTE, 0, timestamp) / 30) * 30, 0) 
                    ORDER BY timestamp ASC
                 )
        FROM evncpc_tb_2
        WHERE timestamp >= @FromDate AND timestamp < @ToDate
    ) t WHERE Rn = 1;

    -- 6. INSERT và NỘI SUY (Forward Fill)
    INSERT INTO evncpc_final_2 (
        timestamp, 
        voltage_AB, voltage_BC, voltage_CA, voltage_LL_avg, 
        voltage_AN, voltage_BN, voltage_CN, voltage_LN_avg, 
        current_A, current_B, current_C, current_avg, 
        power_A, power_B, power_C, power_total, 
        power_factor, energy_kWh, energy_bt, energy_cd, energy_td,
        Is_Interpolated
    )
    SELECT 
        T.Slot,
        ROUND(COALESCE(R.voltage_AB, LastKnown.voltage_AB), 2),
        ROUND(COALESCE(R.voltage_BC, LastKnown.voltage_BC), 2),
        ROUND(COALESCE(R.voltage_CA, LastKnown.voltage_CA), 2),
        ROUND(COALESCE(R.voltage_LL_avg, LastKnown.voltage_LL_avg), 2),
        ROUND(COALESCE(R.voltage_AN, LastKnown.voltage_AN), 2),
        ROUND(COALESCE(R.voltage_BN, LastKnown.voltage_BN), 2),
        ROUND(COALESCE(R.voltage_CN, LastKnown.voltage_CN), 2),
        ROUND(COALESCE(R.voltage_LN_avg, LastKnown.voltage_LN_avg), 2),
        
        ROUND(COALESCE(R.current_A, LastKnown.current_A), 2),
        ROUND(COALESCE(R.current_B, LastKnown.current_B), 2),
        ROUND(COALESCE(R.current_C, LastKnown.current_C), 2),
        ROUND(COALESCE(R.current_avg, LastKnown.current_avg), 2),
        
        ROUND(COALESCE(R.power_A, LastKnown.power_A), 2),
        ROUND(COALESCE(R.power_B, LastKnown.power_B), 2),
        ROUND(COALESCE(R.power_C, LastKnown.power_C), 2),
        ROUND(COALESCE(R.power_total, LastKnown.power_total), 2),
        
        ROUND(COALESCE(R.power_factor, LastKnown.power_factor), 2),
        ROUND(COALESCE(R.energy_kWh, LastKnown.energy_kWh), 2),
        ROUND(COALESCE(R.energy_bt, LastKnown.energy_bt), 2),
        ROUND(COALESCE(R.energy_cd, LastKnown.energy_cd), 2),
        ROUND(COALESCE(R.energy_td, LastKnown.energy_td), 2),

        CASE WHEN R.HasData IS NULL THEN 1 ELSE 0 END 

    FROM @TimeSlots T
    LEFT JOIN @RawUnique R ON T.Slot = R.TimeBucket
    -- Tìm giá trị gần nhất TRƯỚC ĐÓ (Forward Fill)
    OUTER APPLY (
        SELECT TOP 1 *
        FROM evncpc_tb_2 OldData 
        WHERE OldData.timestamp < T.Slot 
        ORDER BY OldData.timestamp DESC
    ) LastKnown;
END;


-- Tự động lấy ngày hôm qua
DECLARE @Yesterday DATE = CAST(GETDATE() - 1 AS DATE);
DECLARE @Today DATE = CAST(GETDATE() AS DATE);

EXEC sp_ETL_Clean_EVN_Data @Yesterday, @Today;