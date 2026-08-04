mkdir %SRC_DIR%\build
cd %SRC_DIR%\build

set BUILD_TYPE=RelWithDebInfo
:: set BUILD_TYPE=Release
:: set BUILD_TYPE=Debug

cmake -G "NMake Makefiles" ^
      -DQMOD=ON ^
      -DCMAKE_BUILD_TYPE=%BUILD_TYPE% ^
      -DCMAKE_INSTALL_PREFIX="%PREFIX%\lib\q\mod\kx" ^
      -DARROW_INSTALL=%BUILD_PREFIX% ^
       %SRC_DIR%
if errorlevel 1 exit \b 1

cmake --build . --config %BUILD_TYPE%
if errorlevel 1 exit \b 1

cmake --build . --config %BUILD_TYPE% --target install
if errorlevel 1 exit \b 1
