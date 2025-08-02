# https://woshub.com/disks-partitions-management-powershell/

$output = @{}
Get-Disk | ForEach-Object {
    $output["$($_.Number)"] = @{
        Number=$_.Number;
        Location=$_.Location;
        Size=$_.Size / (1000 * 1000 * 1000);  # GiB not GB
        SerialNumber=$_.SerialNumber;
        Model=$_.Model;
        Path=$_.Path
    }
}
$output | ConvertTo-JSON
