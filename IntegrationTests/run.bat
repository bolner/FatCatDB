@echo off

set dir=%~dp0
cd %dir%

rmdir /S /Q var\data

bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/add
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query1 >Results\query1.txt
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query2 >Results\query2.txt
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query3 >Results\query3.txt
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query4 >Results\query4.txt

bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/update
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query4 >Results\query4_after_update.txt

bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/queryPlan1 >Results\queryPlan1.txt
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/queryPlan2 >Results\queryPlan2.txt
