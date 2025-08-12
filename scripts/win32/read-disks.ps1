# https://woshub.com/disks-partitions-management-powershell/

$output = @{}
Get-Disk | ForEach-Object {
    $biggest_partition_drive_letter = ((Get-Partition -DiskNumber 0 | Sort-Object -Property Size -Descending).DriveLetter)[0]
    $output["$($_.Number)"] = @{
        Number=$_.Number;
        Location=$_.Location;
        Size=$_.Size / (1000 * 1000 * 1000);  # GiB not GB
        SerialNumber=$_.SerialNumber;
        Model=$_.Model;
        Path=$_.Path;
        DriveLetter=$biggest_partition_drive_letter
    }
}
$output | ConvertTo-JSON
