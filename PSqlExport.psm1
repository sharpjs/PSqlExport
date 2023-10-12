<#
    Part of PSqlExport - Database-to-SQL Export Tool for PowerShell
    https://github.com/sharpjs/PSql.Export

    Copyright 2023 Subatomix Research Inc.
    SPDX-License-Identifier: ISC
#>

#Requires -Version 5
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Dependencies
if (-not (Get-Module PSql)) {
    throw "PowerShell module 'PSql' is required.  Get it here: https://github.com/sharpjs/PSql"
}

# Module scripts
Join-Path $PSScriptRoot *.ps1 -Resolve | ForEach-Object { . $_ }

Export-ModuleMember -Function Export-Sql
