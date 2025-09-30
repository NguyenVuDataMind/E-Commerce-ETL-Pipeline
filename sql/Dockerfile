# Use the official Microsoft SQL Server image as the base
FROM mcr.microsoft.com/mssql/server:2022-latest

# Copy the master setup script and our custom entrypoint script
COPY 00_master_setup.sql /docker-entrypoint-initdb.d/00_master_setup.sql
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# Set our custom script as the new entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

