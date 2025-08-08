# https://woshub.com/disks-partitions-management-powershell/
[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)][String[]]$DriveLetters,
    [Parameter()][int]$GbThreshold=100,
    [Parameter()][switch]$Offline
)

$DriveLetters | ForEach-Object {
    Write-Host "deleting partition $_"
    $part = Get-Partition -DriveLetter $_
    $Gb = $part.Size / (1000 * 1000 * 1000)
    if ($Gb -lt $GbThreshold) {
        Write-Host "skipping partition $_ due to being less than $GbThreshold GB at $Gb GB"
    } else {
        $diskNumber = $part.DiskNumber
        Remove-Partition -DriveLetter $_ -Confirm:$false
        Write-Host "$diskNumber - partitioning as Unknown"
        Set-Disk -Number $diskNumber -PartitionStyle Unknown
        if ($Offline) {
            Write-Host "$diskNumber - setting offline"
            Set-Disk -Number $diskNumber -IsOffline $true
        }
    }
}
exit 0
