# Introduce
从挖财安卓APP导出账本到xls

# APP DB
挖财安卓APP数据库位置：

- 旧版本：`/data/data/com.wacai365/databases/wacai365.so`
- 13.0.9版本及以后：`/data/data/com.wacai365/databases/app_database.db`

注意：获取需要root权限

# 操作命令
- 旧版本：`py.exe .\wacai.py .\wacai365.so`
- 13.0.9版本及以后：`py.exe .\wacai.py .\app_database.db`


# xls
使用挖财官方的xls模板，见`wacai.xls`，生成的xls可以在挖财官网网页上导入

