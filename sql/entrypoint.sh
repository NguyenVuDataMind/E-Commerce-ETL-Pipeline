#!/bin/bash

# Bắt đầu tiến trình SQL Server ở chế độ nền
/opt/mssql/bin/sqlservr &
SQL_SERVER_PID=$!

echo "[Custom Entrypoint] SQL Server process started with PID ${SQL_SERVER_PID}."

# Vòng lặp chờ cho đến khi SQL Server sẵn sàng
echo "[Custom Entrypoint] Waiting for SQL Server to be ready..."
until /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -Q "SELECT 1;" -C -N &>/dev/null; do
    echo "[Custom Entrypoint] SQL Server is unavailable - sleeping"
    sleep 5
done

echo "[Custom Entrypoint] SQL Server is ready. Proceeding with initialization."

# Thực thi kịch bản SQL của chúng ta
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -i /docker-entrypoint-initdb.d/00_master_setup.sql -C -N

if [ $? -eq 0 ]; then
    echo "[Custom Entrypoint] SQL script executed successfully."
else
    echo "[Custom Entrypoint] SQL script execution failed."
fi

# Đưa tiến trình SQL Server trở lại foreground để giữ cho container hoạt động
wait ${SQL_SERVER_PID}
