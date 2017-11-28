<#
    Part of PSqlExport - Database-to-SQL Export Tool for PowerShell
    https://github.com/sharpjs/PSql.Export

    Copyright (C) 2017 Jeffrey Sharp

    Permission to use, copy, modify, and distribute this software for any
    purpose with or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
    MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#>

function Export-Sql {
    <#
    .SYNOPSIS
        Exports a SQL Server or Azure SQL database to a SQL DDL create script.
    #>
    [CmdletBinding(DefaultParameterSetName="Database")]
    param (
        # Exports the named database on the local default instance using integrated authentication.
        [Parameter(Mandatory, Position=0, ParameterSetName="Database")]
        [string] $Database,

        # Exports the connection's current database.
        [Parameter(Mandatory, ParameterSetName="Connection")]
        [System.Data.SqlClient.SqlConnection] $Connection,

        # Exclude schemas where the name matches any of the given LIKE patterns.
        [string[]] $ExcludeSchemas,

        # Exclude objects where the name matches any of the given LIKE patterns.
        [string[]] $ExcludeObjects,

        # Include data in tables where the name matches any of the given LIKE patterns.
        [string[]] $IncludeData
    )

    # Open a connection if one is not already open
    $OwnsConnection = Use-SqlConnection ([ref] $Connection) $Database

    try {
        # Persist parameters in temporary tables
        Invoke-Sql -Connection $Connection -Raw -Sql "
            IF OBJECT_ID('tempdb..#excluded_schemas') IS NOT NULL
                DROP TABLE #excluded_schemas;

            IF OBJECT_ID('tempdb..#excluded_objects') IS NOT NULL
                DROP TABLE #excluded_objects;

            IF OBJECT_ID('tempdb..#populated_tables') IS NOT NULL
                DROP TABLE #populated_tables;

            CREATE TABLE #excluded_schemas
                (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);

            CREATE TABLE #excluded_objects
                (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);

            CREATE TABLE #populated_tables
                (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);
        "
        if ($ExcludeSchemas) {
            Invoke-Sql -Connection $Connection -Sql "
                INSERT #excluded_schemas
                VALUES $( (($ExcludeSchemas | % { $_.Replace("'", "''") } | % { "('$_')" }) -join ", ") );
            "
        }
        if ($ExcludeObjects) {
            Invoke-Sql -Connection $Connection -Sql "
                INSERT #excluded_objects
                VALUES $( (($ExcludeObjects | % { $_.Replace("'", "''") } | % { "('$_')" }) -join ", ") );
            "
        }
        if ($IncludeData) {
            Invoke-Sql -Connection $Connection -Sql "
                INSERT #populated_tables
                VALUES $( (($IncludeData | % { $_.Replace("'", "''") } | % { "('$_')" }) -join ", ") );
            "
        }

        # Execute export script
        $SqlPath = Join-Path $PSScriptRoot Export-Sql.sql
        $Sql = Get-Content -LiteralPath $SqlPath -Encoding UTF8 -Raw
        Invoke-Sql -Connection $Connection -Sql $Sql | % sql
    }
    finally {
        # Close connection if we implicitly opened it
        if ($OwnsConnection) {
            Disconnect-Sql $Connection
        }
    }
}
