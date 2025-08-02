# https://woshub.com/disks-partitions-management-powershell/
[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)][String[]]$DriveLetters,
    [Parameter()][switch]$Offline
)

$DriveLetters | ForEach-Object {
    Write-Host "deleting partition $_"
    $part = Get-Partition -DriveLetter $_
    $diskNumber = $part.DiskNumber
    Remove-Partition -DriveLetter $_ -Confirm:$false
    Write-Host "$diskNumber - partitioning as Unknown"
    Set-Disk -Number $diskNumber -PartitionStyle Unknown
    if ($Offline) {
        Write-Host "$diskNumber - setting offline"
        Set-Disk -Number $diskNumber -IsOffline $true
    }
}
