@echo off

set dir=%~dp0
cd %dir%

rmdir /S /Q var\data

bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/add
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query1 >Results\query1.txt
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query2 >Results\query2.txt
bin\Release\netcoreapp3.1\publish\IntegrationTests.exe test/query3 >Results\query3.txt
