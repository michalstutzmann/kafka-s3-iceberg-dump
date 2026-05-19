@REM Minimal Apache Maven Wrapper (Windows).
@REM Local builds only; the Docker `builder` service does not need this.
@echo off
setlocal
set "DIR=%~dp0"
for /f "tokens=2 delims==" %%a in ('findstr /b "distributionUrl=" "%DIR%.mvn\wrapper\maven-wrapper.properties"') do set "DIST_URL=%%a"
for %%F in ("%DIST_URL%") do set "ZIPNAME=%%~nF"
set "MVN_VERSION=%ZIPNAME:apache-maven-=%"
set "MVN_VERSION=%MVN_VERSION:-bin=%"
set "WRAPPER_HOME=%USERPROFILE%\.m2\wrapper\dists\apache-maven-%MVN_VERSION%"
if not exist "%WRAPPER_HOME%\bin\mvn.cmd" (
  echo Downloading Apache Maven %MVN_VERSION% ...
  if exist "%WRAPPER_HOME%.tmp" rmdir /s /q "%WRAPPER_HOME%.tmp"
  mkdir "%WRAPPER_HOME%.tmp"
  powershell -Command "Invoke-WebRequest -Uri '%DIST_URL%' -OutFile '%WRAPPER_HOME%.tmp\maven.zip'"
  powershell -Command "Expand-Archive -Path '%WRAPPER_HOME%.tmp\maven.zip' -DestinationPath '%WRAPPER_HOME%.tmp'"
  move "%WRAPPER_HOME%.tmp\apache-maven-%MVN_VERSION%" "%WRAPPER_HOME%"
  rmdir /s /q "%WRAPPER_HOME%.tmp"
)
"%WRAPPER_HOME%\bin\mvn.cmd" %*
