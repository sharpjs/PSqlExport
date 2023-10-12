<#
    Part of PSqlExport - Database-to-SQL Export Tool for PowerShell
    https://github.com/sharpjs/PSql.Export

    Copyright 2023 Subatomix Research Inc.
    SPDX-License-Identifier: ISC
#>

function Export-Sql {
    <#
    .SYNOPSIS
        Exports a SQL Server or Azure SQL database to a SQL DDL creation script.
    #>
    [CmdletBinding(DefaultParameterSetName = "Local")]
    param (
        # Object specifying how to connect to the database to export.  Obtain via the New-SqlContext command.  If not provided, the command targets the default SQL Server instance on the local machine, using integrated authentication.  This parameter supports pipeline input.
        [Parameter(Mandatory, Position = 0, ParameterSetName = "Context", ValueFromPipeline)]
        [PSql.SqlContext]
        $Context
    ,
        # Name of the database to export.  If not provided, the command targets the default database of -Context.
        [Parameter(           Position = 1, ParameterSetName = "Context")]
        [Parameter(Mandatory, Position = 0, ParameterSetName = "Local")]
        [ValidateNotNullOrEmpty()]
        [string]
        $DatabaseName
    ,
        # Exclude schemas where the name matches any of the given LIKE patterns.
        [Parameter()]
        [ValidateNotNull()]
        [AllowEmptyCollection()]
        [string[]]
        $ExcludeSchemas
    ,
        # Exclude objects where the name matches any of the given LIKE patterns.
        [Parameter()]
        [ValidateNotNull()]
        [AllowEmptyCollection()]
        [string[]]
        $ExcludeObjects
    ,
        # Exclude data in tables where the name matches any of the given LIKE patterns.
        [Parameter()]
        [ValidateNotNull()]
        [AllowEmptyCollection()]
        [string[]]
        $ExcludeData
    )

    process {
        $Connection = $null

        $Context ??= New-SqlContext

        if ($DatabaseName) {
            $Context = $Context[$DatabaseName]
        }

        try {
            $Connection = $Context | Connect-Sql

            # Persist parameters in temporary tables
            if ($ExcludeSchemas) {
                Invoke-Sql -Connection $Connection "
                    CREATE TABLE #excluded_schemas
                        (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);
                    INSERT #excluded_schemas
                    VALUES $((
                        (
                            $ExcludeSchemas `
                                | ForEach-Object { $_.Replace("'", "''") } `
                                | ForEach-Object { "('$_')" }
                        ) -join ", "
                    ));
                "
            }
            if ($ExcludeObjects) {
                Invoke-Sql -Connection $Connection "
                    CREATE TABLE #excluded_objects
                        (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);
                    INSERT #excluded_objects
                    VALUES $((
                        (
                            $ExcludeObjects `
                                | ForEach-Object { $_.Replace("'", "''") } `
                                | ForEach-Object { "('$_')" }
                        ) -join ", "
                    ));
                "
            }
            if ($ExcludeData) {
                Invoke-Sql -Connection $Connection "
                    CREATE TABLE #excluded_data
                        (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);
                    INSERT #excluded_data
                    VALUES $((
                        (
                            $ExcludeData `
                                | ForEach-Object { $_.Replace("'", "''") } `
                                | ForEach-Object { "('$_')" }
                        ) -join ", "
                    ));
                "
            }

            # Execute export script
            $SqlPath = Join-Path $PSScriptRoot Export-Sql.sql
            $Sql     = Get-Content -LiteralPath $SqlPath -Raw
            Invoke-Sql -Connection $Connection -Sql $Sql | ForEach-Object sql
        }
        finally {
            Disconnect-Sql $Connection
        }
    }
}
