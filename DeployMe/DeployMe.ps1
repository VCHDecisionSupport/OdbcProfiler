# Param 
# (
#     [parameter(Mandatory = $true)]
#     [ValidateSet('STDBDECSUP01','STDBDECSUP02','STDBDECSUP03','SPDBDECSUP04','PC')]
#     [string] $DeploymentSqlServer
# )
Set-Location -Path $PSScriptRoot

# # # # these if's are ran when [parameter(Mandatory = $false)]
if($DeploymentSqlServer -eq "")
{
    $LocalDeploymentUserInput = Read-Host -Prompt ("`nNo deployment server specified.`n`tDefault to local machine ({0})?  Y/n" -f $env:ComputerName)
    $LocalDeployment = ($LocalDeploymentUserInput -eq "Y")
    Write-Host $LocalDeployment
}
if($LocalDeployment -eq $true)
{
    $DeploymentSqlServer = $env:ComputerName
}


Write-Host ("`n`n(re)Deploying GenericProfiles DDL SQL scripts at:`n`t{1}`n to Sql Server: `n`t{0}" -f $DeploymentSqlServer, $PSScriptRoot) 

# hard-code ordered deployment scripts
$sql_scripts = New-Object System.Collections.ArrayList

# GenericProfiles
# database
[void]$sql_scripts.Add("Database/GenericProfiles.sql")
# profile tables
[void]$sql_scripts.Add("Database/Table/dbo.ServerInfo.sql")
[void]$sql_scripts.Add("Database/Table/dbo.DatabaseInfo.sql")
[void]$sql_scripts.Add("Database/Table/dbo.ViewTableInfo.sql")
[void]$sql_scripts.Add("Database/Table/dbo.ViewTableProfile.sql")


foreach($sql_script in $sql_scripts)
{
    Write-Host $sql_script
    SQLCMD -S $DeploymentSqlServer -E -i $sql_script
}

