# https://woshub.com/disks-partitions-management-powershell/

$output = @{}
Get-Partition | ForEach-Object {
    $output["$($_.DriveLetter)"] = @{
        PartitionNumber=$_.PartitionNumber;
        DriveLetter=$_.DriveLetter;
        DiskNumber=$_.DiskNumber;
        DiskPath=$_.DiskPath
    }
}
# ignore tiny reserved partitions
if ($output.ContainsKey("`0")) {
    $output.Remove("`0")
}
$output | ConvertTo-JSON
