# https://ss64.com/ps/syntax-elevate.html
function isadmin {
    return ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}
If (-NOT (isadmin)) {
    Write-Warning "Not admin!"
    exit 1
}
Write-Host "Admin!"