# https://woshub.com/disks-partitions-management-powershell/


Get-Disk | Where-Object IsOffline -Eq $True | Set-Disk -IsOffline $False
$diskNumbers = (Get-Disk | Select-Object DiskNumber).DiskNumber
$partitionDiskNumbers = (Get-Partition | Where-Object {$_.DriveLetter -Match "[A-Z]"} | Select-Object DiskNumber).DiskNumber
$unpartitionedDiskNumbers = $diskNumbers | Where-Object {$partitionDiskNumbers -NotContains $_}

$newDiskNumberToDriveLetter = @{}
$unpartitionedDiskNumbers | ForEach-Object {
    $disk = Get-Disk -Number $_
    Write-Host "disk $_"
    $disk
    Write-Host "initialized"
    Initialize-Disk -Number $_ -ErrorAction SilentlyContinue
    Write-Host "setting to GPT"
    Set-Disk -Number $_ -PartitionStyle GPT
    Write-Host "new partition"
    $part = New-Partition -DiskNumber $_ -AssignDriveLetter -UseMaximumSize
    $part
    $newDiskNumberToDriveLetter["$_"] = $part.DriveLetter
    # Write-Host "activating parition: $($part.DriveLetter)"
    # Set-Partition -DriveLetter $part.DriveLetter -IsActive $true
    Write-Host "formatting NTFS"
    Format-Volume -DriveLetter $part.DriveLetter -FileSystem NTFS -NewFileSystemLabel $part.DriveLetter -Confirm:$false
}
Write-Host "Begin Output Parsing Here:"
Write-Host ($newDiskNumberToDriveLetter | ConvertTo-JSON)
