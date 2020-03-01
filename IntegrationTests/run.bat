@echo off

set dir=%~dp0
cd %dir%

rmdir /S /Q var\data

set ExecPath="bin\Release\netcoreapp3.1\publish\IntegrationTests.exe"

%ExecPath% test/add
%ExecPath% test/query1 >Results\query1.txt
%ExecPath% test/query2 >Results\query2.txt
%ExecPath% test/query3 >Results\query3.txt
%ExecPath% test/query4 >Results\query4.txt

%ExecPath% test/update
%ExecPath% test/query4 >Results\query4_after_update.txt

%ExecPath% test/queryPlan1 >Results\queryPlan1.txt
%ExecPath% test/queryPlan2 >Results\queryPlan2.txt

rmdir /S /Q var\data

%ExecPath% bookmarkTest/add
%ExecPath% bookmarkTest/queryFull >Results\bookmark1_Full.txt
%ExecPath% bookmarkTest/queryPage1 >Results\bookmark2_Page1.txt
%ExecPath% bookmarkTest/queryPage2 >Results\bookmark3_Page2.txt
