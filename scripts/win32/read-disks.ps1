# https://woshub.com/disks-partitions-management-powershell/

$output = @{}
Get-Disk | ForEach-Object {
    $partition = Get-Partition -DiskNumber $_.Number -ErrorAction SilentlyContinue
    if ($null -ne $partition) {
        $biggest_partition_drive_letter = (($partition | Sort-Object -Property Size -Descending).DriveLetter)[0]
    } else {
        $biggest_partition_drive_letter = $null
    }

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
