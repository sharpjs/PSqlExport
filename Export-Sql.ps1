<#
    Part of PSqlExport - Database-to-SQL Export Tool for PowerShell
    https://github.com/sharpjs/PSql.Export

    Copyright 2023 Subatomix Research Inc.
    SPDX-License-Identifier: ISC
#>

function Export-Sql {
    <#
    .SYNOPSIS
        Exports a SQL Server or Azure SQL database to a SQL DDL create script.
    #>
    [CmdletBinding(DefaultParameterSetName="Database")]
    param (
        # Name of the database to export on the local default SQL Server instance using integrated authentication.
        [Parameter(Mandatory, Position=0, ParameterSetName="Database")]
        [string] $Database,

        # Object specifying how to connect to the database to export.  Created by New-SqlConnectionInfo.
        [Parameter(Mandatory, ParameterSetName="Source", ValueFromPipeline)]
        [PSCustomObject] $Source,

        # Exclude schemas where the name matches any of the given LIKE patterns.
        [string[]] $ExcludeSchemas,

        # Exclude objects where the name matches any of the given LIKE patterns.
        [string[]] $ExcludeObjects,

        # Exclude data in tables where the name matches any of the given LIKE patterns.
        [string[]] $ExcludeData
    )

    $Connection = $null

    if ($Database) {
        $Source = New-SqlConnectionInfo . $Database
    }

    try {
        $Connection = $Source | Connect-Sql

        # Persist parameters in temporary tables
        Invoke-Sql -Connection $Connection -Raw "
            IF OBJECT_ID('tempdb..#excluded_schemas') IS NOT NULL
                DROP TABLE #excluded_schemas;

            IF OBJECT_ID('tempdb..#excluded_objects') IS NOT NULL
                DROP TABLE #excluded_objects;

            IF OBJECT_ID('tempdb..#excluded_data') IS NOT NULL
                DROP TABLE #excluded_data;

            CREATE TABLE #excluded_schemas
                (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);

            CREATE TABLE #excluded_objects
                (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);

            CREATE TABLE #excluded_data
                (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);
        "
        if ($ExcludeSchemas) {
            Invoke-Sql -Connection $Connection "
                INSERT #excluded_schemas
                VALUES $( (($ExcludeSchemas | ForEach-Object { $_.Replace("'", "''") } | ForEach-Object { "('$_')" }) -join ", ") );
            "
        }
        if ($ExcludeObjects) {
            Invoke-Sql -Connection $Connection "
                INSERT #excluded_objects
                VALUES $( (($ExcludeObjects | ForEach-Object { $_.Replace("'", "''") } | ForEach-Object { "('$_')" }) -join ", ") );
            "
        }
        if ($ExcludeData) {
            Invoke-Sql -Connection $Connection "
                INSERT #excluded_data
                VALUES $( (($ExcludeData | ForEach-Object { $_.Replace("'", "''") } | ForEach-Object { "('$_')" }) -join ", ") );
            "
        }

        # Execute export script
        $SqlPath = Join-Path $PSScriptRoot Export-Sql.sql
        $Sql = Get-Content -LiteralPath $SqlPath -Encoding UTF8 -Raw
        Invoke-Sql -Connection $Connection -Sql $Sql | ForEach-Object sql
    }
    finally {
        Disconnect-Sql $Connection
    }
}
