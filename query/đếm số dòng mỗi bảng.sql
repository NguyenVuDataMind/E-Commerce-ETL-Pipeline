-- Đếm số dữ liệu của tất cả các bảng
SELECT 
    t.TABLE_SCHEMA AS 'Schema',
    t.TABLE_NAME AS 'Tên bảng',
    p.rows AS 'Số dòng dữ liệu'
FROM INFORMATION_SCHEMA.TABLES t
INNER JOIN sys.partitions p ON t.TABLE_NAME = OBJECT_NAME(p.object_id)
WHERE t.TABLE_TYPE = 'BASE TABLE'
    AND p.index_id IN (0, 1) -- Chỉ lấy heap hoặc clustered index
ORDER BY p.rows DESC;