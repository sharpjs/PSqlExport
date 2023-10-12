/*
    Database-to-SQL Export Script
    https://github.com/sharpjs/PSql.Export

    Copyright 2023 Subatomix Research Inc.
    SPDX-License-Identifier: ISC

    PHASES
    ======
    * Setup
    * Database Options
    * Users & Roles
    * Role Membership
    * Schemas
    * Scalar Types
    * Table Types
    * Tables (no computed columns or constraints)
    * Table Data
    * Computed Columns, Views, Funcs, Procs
    * Primary Key, Unique Constraints, Indexes
    * Constraints: foreign key
    * Constraints: default
    * Constraints: check
    * Extended Properties
    * Triggers
*/

-- -----------------------------------------------------------------------------
-- Setup

SET NOCOUNT ON;

DECLARE
    -- General Constants
    @NL         nchar(2) = NCHAR(13) + NCHAR(10),
    @Edition    int      = CONVERT(int, SERVERPROPERTY('EngineEdition')),
    -- Database Properties
    @Collation  sysname  ,
    @Version    int      ,
    -- Edition Contants
    -- https://docs.microsoft.com/en-us/sql/t-sql/functions/serverproperty-transact-sql
    @Standard   int      =  2, -- Standard, Web, Business Intelligence
    @Enterprise int      =  3, -- Evaluation, Developer, Enterprise
    @Express    int      =  4, -- Express
    @AzureSqlDb int      =  5, -- Azure SQL Database
    @AzureSynAn int      =  6, -- Azure Synapse Analytics (fka Data Warehouse)
    @AzureSqlMi int      =  8, -- Azure SQL Managed Instance
    @AzureSqlEd int      =  9, -- Azure SQL Edge
    @AzureSynSl int      = 11, -- Azure Synapse serverless SQL pool
    -- Version Constants
    @Sql2008    int      = 100, -- SQL Server 2008 and 2008 R2
    @Sql2012    int      = 110, -- SQL Server 2012
    @Sql2014    int      = 120, -- SQL Server 2014
    @Sql2016    int      = 130, -- SQL Server 2016
    @Sql2017    int      = 140, -- SQL Server 2017
    @Sql2019    int      = 150, -- SQL Server 2019
    @Sql2022    int      = 160  -- SQL Server 2022 & Azure SQL
;

SELECT
    @Version   = compatibility_level,
    @Collation = collation_name
FROM
    sys.databases
WHERE
    name = DB_NAME()
;

IF OBJECT_ID('tempdb..#excluded_schemas') IS NULL
    CREATE TABLE #excluded_schemas
        (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);

IF OBJECT_ID('tempdb..#excluded_objects') IS NULL
    CREATE TABLE #excluded_objects
        (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);

IF OBJECT_ID('tempdb..#excluded_data') IS NULL
    CREATE TABLE #excluded_data
        (pattern sysname COLLATE CATALOG_DEFAULT NOT NULL PRIMARY KEY);

IF OBJECT_ID('tempdb..#steps') IS NOT NULL
    DROP TABLE #steps;

CREATE TABLE #steps
(
    step_id     int IDENTITY    NOT NULL,
    kind        sysname         NOT NULL,
    name        nvarchar(257)   NOT NULL, -- max schema.object length
    sql         nvarchar(max)   NOT NULL,

    PRIMARY KEY (step_id)
);

-- -----------------------------------------------------------------------------
-- Schema Lookup
--
-- Contains only the schemas selected for export.

IF OBJECT_ID('tempdb..#schemas') IS NOT NULL
    DROP TABLE #schemas;

CREATE TABLE #schemas
(
    schema_id   int                                 NOT NULL,
    name        sysname COLLATE CATALOG_DEFAULT     NOT NULL,

    display_name    AS           name,
    quoted_name     AS QUOTENAME(name),

    PRIMARY KEY (schema_id),
);

CREATE UNIQUE INDEX ux_1
    ON #schemas (name);

INSERT #schemas
SELECT
    s.schema_id, s.name
FROM
    sys.schemas s
WHERE 0=0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    AND NOT EXISTS (SELECT 0 FROM #excluded_schemas x WHERE s.name LIKE x.pattern)
;

-- -----------------------------------------------------------------------------
-- Schema Lookup
--
-- Contains only the schemas selected for export.

DECLARE @schemas TABLE
(
    schema_id   int         NOT NULL,
    name        sysname     COLLATE CATALOG_DEFAULT NOT NULL,

    PRIMARY KEY         (schema_id),
    INDEX ux_1 UNIQUE   (name)
);

INSERT @schemas
SELECT
    s.schema_id, s.name
FROM
    sys.schemas s
WHERE 0=0
    AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    AND NOT EXISTS (SELECT 0 FROM #excluded_schemas x WHERE s.name LIKE x.pattern)
;

-- -----------------------------------------------------------------------------
-- Object Lookup
--
-- Contains only the objects selected for export.

IF OBJECT_ID('tempdb..#objects') IS NOT NULL
    DROP TABLE #objects;

CREATE TABLE #objects
(
    object_id   int                                 NOT NULL,
    schema_id   int                                 NOT NULL,
    type        char(2) COLLATE CATALOG_DEFAULT     NOT NULL,
    schema_name sysname COLLATE CATALOG_DEFAULT     NOT NULL,
    object_name sysname COLLATE CATALOG_DEFAULT     NOT NULL,

    display_name    AS           schema_name  + '.' +           object_name,
    quoted_name     AS QUOTENAME(schema_name) + '.' + QUOTENAME(object_name),

    PRIMARY KEY (object_id),
);

CREATE UNIQUE INDEX ux_1
    ON #objects (schema_name, object_name)
    INCLUDE     (type, schema_id);

CREATE UNIQUE INDEX ux_2
    ON #objects (type, schema_name, object_name)
    INCLUDE     (schema_id);

INSERT #objects
SELECT
    o.object_id,
    o.schema_id,
    o.type,
    schema_name = s.name,
    object_name = o.name
FROM
    #schemas s
INNER JOIN
    sys.objects o
    ON o.schema_id = s.schema_id
LEFT JOIN
    sys.tables t
    ON  t.object_id = o.object_id
    AND 'U'         = o.type
WHERE 0=0
    AND o.type IN (
    --  'AF', -- Aggregate function (CLR)
        'C',  -- CHECK constraint
        'D',  -- DEFAULT constraint (or stand-alone)
        'F',  -- FOREIGN KEY constraint
        'FN', -- SQL scalar function
    --  'FS', -- Assembly (CLR) scalar-function
    --  'FT', -- Assembly (CLR) table-valued function
        'IF', -- SQL inline table-valued function
    --  'IT', -- Internal table
        'P',  -- SQL Stored Procedure
    --  'PC', -- Assembly (CLR) stored-procedure
    --  'PG', -- Plan guide
        'PK', -- PRIMARY KEY constraint
    --  'R',  -- Rule (stand-alone)
    --  'RF', -- Replication-filter-procedure
    --  'S',  -- System base table
    --  'SN', -- Synonym
    --  'SO', -- Sequence object
        'U',  -- Table (user-defined)
        'V',  -- View
    --  'SQ', -- Service queue
    --  'TA', -- Assembly (CLR) DML trigger
        'TF', -- SQL table-valued-function
        'TR', -- SQL DML trigger
    --  'TT', -- Table type
        'UQ'  -- UNIQUE constraint
    --  'X'   -- Extended stored procedure
    )
    AND        o.is_ms_shipped    = 0
    AND ISNULL(t.is_external,  0) = 0
    AND ISNULL(t.is_filetable, 0) = 0
    AND NOT EXISTS (SELECT 0 FROM #excluded_objects x WHERE s.name + '.' + o.name LIKE x.pattern)
;

-- -----------------------------------------------------------------------------
-- Type Lookup
--
-- * column    -> user type name
-- * user type -> system type name

DECLARE @types TABLE
(
    major_id    int             NOT NULL, -- object_id or user_type_id
    minor_id    int             NOT NULL, -- column_id or 0
    type        nvarchar(517)   NOT NULL, -- max [quoted].[name] length

    PRIMARY KEY (major_id, minor_id)
);

WITH tableish_objects AS
(
    -- Tables
    SELECT object_id
    FROM @schemas s
    INNER JOIN sys.tables t ON t.schema_id = s.schema_id
    WHERE t.is_ms_shipped = 0
  UNION ALL
    -- Table Types
    SELECT object_id = t.type_table_object_id
    FROM @schemas s
    INNER JOIN sys.table_types t ON t.schema_id = s.schema_id
    WHERE t.is_user_defined = 1
)
, types AS
(
    -- User-defined scalar types
    SELECT
        major_id    = u.user_type_id,
        minor_id    = 0,
        schema_name = NULL,
        t.name, u.max_length, u.precision, u.scale, t.is_user_defined
    FROM
        @schemas s
    INNER JOIN
        sys.types u -- user type
        ON u.schema_id = s.schema_id
    INNER JOIN
        sys.types t -- system type
        ON t.user_type_id = u.system_type_id
    WHERE 0=0
        AND u.is_user_defined  = 1
        AND u.is_assembly_type = 0
        AND u.is_table_type    = 0
  UNION ALL
    -- Columns of tables and table types
    SELECT
        major_id    = o.object_id,
        minor_id    = c.column_id,
        schema_name = IIF(t.is_user_defined = 1, SCHEMA_NAME(t.schema_id), NULL),
        t.name, c.max_length, c.precision, c.scale, t.is_user_defined
    FROM
        tableish_objects o
    INNER JOIN
        sys.columns c
        ON c.object_id = o.object_id
    INNER JOIN
        sys.types t ON
        t.user_type_id = c.user_type_id
)
INSERT @types
SELECT
    major_id,
    minor_id,
    type = CASE is_user_defined
        WHEN 1 THEN
            ISNULL(QUOTENAME(schema_name) + '.', '') + QUOTENAME(name)
        ELSE
            name +
            CASE name
                WHEN 'float'          THEN '(' + CONVERT(nvarchar, precision) + ')'
                WHEN 'decimal'        THEN '(' + CONVERT(nvarchar, precision) + ', ' + CONVERT(nvarchar, scale) + ')'
                WHEN 'numeric'        THEN '(' + CONVERT(nvarchar, precision) + ', ' + CONVERT(nvarchar, scale) + ')'
                WHEN 'time'           THEN '(' + CONVERT(nvarchar, scale) + ')'
                WHEN 'datetime2'      THEN '(' + CONVERT(nvarchar, scale) + ')'
                WHEN 'datetimeoffset' THEN '(' + CONVERT(nvarchar, scale) + ')'
                WHEN 'binary'         THEN '(' + IIF(max_length < 1, 'max', CONVERT(nvarchar, max_length))   + ')'
                WHEN 'varbinary'      THEN '(' + IIF(max_length < 1, 'max', CONVERT(nvarchar, max_length))   + ')'
                WHEN 'char'           THEN '(' + IIF(max_length < 1, 'max', CONVERT(nvarchar, max_length))   + ')'
                WHEN 'nchar'          THEN '(' + IIF(max_length < 1, 'max', CONVERT(nvarchar, max_length/2)) + ')'
                WHEN 'varchar'        THEN '(' + IIF(max_length < 1, 'max', CONVERT(nvarchar, max_length))   + ')'
                WHEN 'nvarchar'       THEN '(' + IIF(max_length < 1, 'max', CONVERT(nvarchar, max_length/2)) + ')'
                ELSE ''
            END
    END
FROM
    types
;

-- -----------------------------------------------------------------------------
-- Database Options

INSERT #steps (kind, name, sql)
SELECT
    'opts', DB_NAME(),
    sql =
    'ALTER DATABASE CURRENT'                                                                               + @NL +
    '    COLLATE ' + collation_name + ';'                                                                  + @NL +
    ''                                                                                                     + @NL +
    'ALTER DATABASE CURRENT SET'                                                                           + @NL +
    '    ANSI_NULL_DEFAULT            ' + IIF(is_ansi_null_default_on       = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    ANSI_NULLS                   ' + IIF(is_ansi_nulls_on              = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    ANSI_PADDING                 ' + IIF(is_ansi_padding_on            = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    ANSI_WARNINGS                ' + IIF(is_ansi_warnings_on           = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    QUOTED_IDENTIFIER            ' + IIF(is_quoted_identifier_on       = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    ARITHABORT                   ' + IIF(is_arithabort_on              = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    NUMERIC_ROUNDABORT           ' + IIF(is_numeric_roundabort_on      = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    CONCAT_NULL_YIELDS_NULL      ' + IIF(is_concat_null_yields_null_on = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    RECURSIVE_TRIGGERS           ' + IIF(is_recursive_triggers_on      = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    CURSOR_CLOSE_ON_COMMIT       ' + IIF(is_cursor_close_on_commit_on  = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    PARAMETERIZATION             ' + IIF(is_parameterization_forced    = 1, 'FORCED', 'SIMPLE') + ',' + @NL +
    '    AUTO_SHRINK                  ' + IIF(is_auto_shrink_on             = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    AUTO_CREATE_STATISTICS       ' + IIF(is_auto_create_stats_on       = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    AUTO_UPDATE_STATISTICS       ' + IIF(is_auto_update_stats_on       = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    AUTO_UPDATE_STATISTICS_ASYNC ' + IIF(is_auto_update_stats_async_on = 1, 'ON',     'OFF'   ) + ',' + @NL +
    '    DELAYED_DURABILITY =         ' + CASE delayed_durability
                                            WHEN 1 THEN 'ALLOWED'
                                            WHEN 2 THEN 'FORCED'
                                                   ELSE 'DISABLED'
                                          END                                                        + ';' + @NL +
    ''                                                                                                     + @NL +
    'ALTER DATABASE CURRENT SET'                                                                           + @NL +
    '    ALLOW_SNAPSHOT_ISOLATION     ' + IIF(snapshot_isolation_state&1    = 1, 'ON',     'OFF'   ) + ';' + @NL +
    ''                                                                                                     + @NL +
    'ALTER DATABASE CURRENT SET'                                                                           + @NL +
    '    READ_COMMITTED_SNAPSHOT      ' + IIF(is_read_committed_snapshot_on = 1, 'ON',     'OFF'   ) + ';' + @NL +
    IIF(@Edition IN (@Express, @Standard, @Enterprise, @AzureSqlMi),
    ''                                                                                                     + @NL +
    'IF CONVERT(int, SERVERPROPERTY(''EngineEdition'')) IN (' +
        CONCAT(@Express, ', ', @Standard, ', ', @Enterprise, ', ', @AzureSqlMi) +
    ')'                                                                                                    + @NL +
    'EXEC('''                                                                                              + @NL +
    '    ALTER DATABASE CURRENT SET'                                                                       + @NL +
    '        RECOVERY                 ' + CASE recovery_model
                                            WHEN 1 THEN 'FULL'
                                            WHEN 2 THEN 'BULK_LOGGED'
                                                   ELSE 'SIMPLE'
                                          END                                                        + ',' + @NL +
    '        CONTAINMENT =            ' + CASE containment
                                            WHEN 0 THEN 'NONE'
                                                   ELSE 'PARTIAL'
                                          END                                                        + ';' + @NL +
    ''');'                                                                                                 + @NL ,
    ''
    ) +
    ''
    -- FUTURE: query store?
    -- FUTURE: memory optimized elevate?
    -- FUTURE: other on-prem-only properties
FROM
    sys.databases
WHERE
    database_id = DB_ID()
;

-- -----------------------------------------------------------------------------
-- Users & Roles
--
-- UNSUPPORTED: Types A, C, E, G, K, U, X

INSERT #steps (kind, name, sql)
SELECT
    kind  = IIF(p.type = 'R', 'role', 'user'),
    name  = p.name,
    sql   = REPLACE(
        CASE
        WHEN p.type = 'R' THEN
            'CREATE ROLE {name} AUTHORIZATION dbo;' + @NL
        WHEN p.type = 'S' AND p.authentication_type = 2 THEN
            'CREATE USER {name} WITH PASSWORD = ''' + CONVERT(sysname, NEWID()) + ''';' + @NL
        ELSE
            'CREATE USER {name} WITHOUT LOGIN;' + @NL
        END,
        '{name}', QUOTENAME(p.name)
    )
FROM
    sys.database_principals p
WHERE 0=0
    AND p.type IN ('R', 'S')
    AND p.name NOT IN ('dbo', 'public', 'guest', 'sys', 'INFORMATION_SCHEMA')
    AND p.is_fixed_role = 0
ORDER BY
    p.name
;

-- -----------------------------------------------------------------------------
-- Role Membership

INSERT #steps (kind, name, sql)
SELECT
    kind  = 'member',
    name  = u.name + ' in ' + r.name,
    'ALTER ROLE ' + QUOTENAME(r.name) + ' ' +
    'ADD MEMBER ' + QUOTENAME(u.name) + ';' + @NL
FROM
    sys.database_role_members m
INNER JOIN
    sys.database_principals r
    ON r.principal_id = m.role_principal_id
INNER JOIN
    sys.database_principals u
    ON u.principal_id = m.member_principal_id
WHERE 0=0
    AND r.type = 'R'
    AND u.type = 'S'
    AND u.name NOT IN ('dbo', 'public', 'guest', 'sys', 'INFORMATION_SCHEMA')
ORDER BY
    r.name,
    u.name
;

-- -----------------------------------------------------------------------------
-- Schemas

INSERT #steps (kind, name, sql)
SELECT
    'schema', s.name,
    'CREATE SCHEMA ' + QUOTENAME(s.name) + ' AUTHORIZATION dbo;' + @NL
FROM
    sys.schemas s
WHERE 0=0
    -- Exclude built-in schemas
    AND s.name NOT IN ('dbo', 'guest', 'sys', 'INFORMATION_SCHEMA')
    AND s.name NOT LIKE 'db[_]%'
    AND NOT EXISTS (SELECT 0 FROM #excluded_schemas x WHERE s.name LIKE x.pattern)
ORDER BY
    name
;

-- -----------------------------------------------------------------------------
-- Scalar Types

INSERT #steps (kind, name, sql)
SELECT
    'type', s.name + '.' + u.name,
    --
    'CREATE TYPE ' + QUOTENAME(s.name) + '.' + QUOTENAME(u.name) + ' '
  + 'FROM ' + t.type
  + IIF(u.is_nullable = 1, ' NULL', ' NOT NULL')
  + ';' + @NL
FROM
    @schemas s
INNER JOIN
    sys.types u
    ON u.schema_id = s.schema_id
INNER JOIN
    @types t
    ON  t.major_id = u.user_type_id
    AND t.minor_id = 0 -- indicates UDT
ORDER BY
    s.name, u.name
;

-- -----------------------------------------------------------------------------
-- Table Types

INSERT #steps (kind, name, sql)
SELECT
    'type', s.name + '.' + t.name,
    --
    'CREATE TYPE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ' '
  + 'AS TABLE' + @NL +
  + '(' + @NL
  + STUFF((
        SELECT ',' + @NL
          + '    '
          + QUOTENAME(c.name)
          + REPLICATE(' ', 2 +
                MAX(LEN(QUOTENAME(c.name))) OVER ()
                  - LEN(QUOTENAME(c.name))
            )
          + ty.type
          + REPLICATE(' ', 2 +
                MAX(LEN(ty.type)) OVER ()
                  - LEN(ty.type)
            )
          + ISNULL(' COLLATE ' + NULLIF(c.collation_name, @Collation), '')
          + IIF(c.is_nullable   = 1, IIF(c.is_computed = 0, ' NULL', ''), ' NOT NULL')
          + ISNULL(' DEFAULT ' + d.definition, '')
          + ISNULL(' IDENTITY('
              + CONVERT(nvarchar(max), i.seed_value)      + ', '
              + CONVERT(nvarchar(max), i.increment_value) + ')'
              , '')
          + IIF(c.is_rowguidcol = 1, ' ROWGUIDCOL', '')
        FROM
            sys.columns c
        INNER JOIN
            @types ty
            ON  ty.major_id = c.object_id
            AND ty.minor_id = c.column_id
        LEFT JOIN
            sys.identity_columns i
            ON  i.object_id = c.object_id
            AND i.column_id = c.column_id
        LEFT JOIN
            sys.default_constraints d
            ON  d.parent_object_id = c.object_id
            AND d.parent_column_id = c.column_id
        WHERE 0=0
            AND c.object_id = t.type_table_object_id
            AND c.is_computed   = 0
            AND c.is_column_set = 0
        ORDER BY
            c.column_id
        FOR XML
            PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 3, '')
  + ISNULL((
        SELECT ',' + @NL
          + '    '
          + CASE
                WHEN i.is_primary_key = 1 THEN
                    'PRIMARY KEY'
                  + IIF(i.index_id > 1, ' NONCLUSTERED', '')
                  + ' ('
                WHEN i.is_unique_constraint = 1 THEN
                    'UNIQUE'
                  + IIF(i.index_id = 1, ' CLUSTERED', '')
                  + ' ('
                ELSE
                    'INDEX ' + QUOTENAME(i.name)
                  + IIF(i.is_unique = 1, ' UNIQUE',    '')
                  + IIF(i.index_id  = 1, ' CLUSTERED', '')
                  + ' ('
            END
          + STUFF((
                SELECT ', ' + QUOTENAME(icc.name)
                  + IIF(ic.is_descending_key = 1, ' DESC', '')
                FROM sys.index_columns ic
                INNER JOIN sys.columns icc ON icc.object_id = ic.object_id AND icc.column_id = ic.column_id
                WHERE ic.object_id = i.object_id
                  AND ic.index_id  = i.index_id
                ORDER BY ic.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'nvarchar(max)'), 1, 2, '')
          + ')'
        FROM
            sys.indexes i
        WHERE 0=0
            AND i.object_id = t.type_table_object_id
            AND i.type IN (1, 2) -- Clustered, Nonclustered
        ORDER BY
            i.index_id
        FOR XML
            PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), '')
  + ISNULL((
        SELECT ',' + @NL
          + '    CHECK ' + k.definition
        FROM
            sys.check_constraints k
        WHERE 0=0
            AND k.parent_object_id = t.type_table_object_id
        ORDER BY
            k.object_id
        FOR XML
            PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), '')
  + @NL
  + ');' + @NL
FROM
    @schemas s
INNER JOIN
    sys.table_types t
    ON t.schema_id = s.schema_id
ORDER BY
    s.name, t.name
;

-- -----------------------------------------------------------------------------
-- Interdependent Objects:
--   Tables (Due to Computed Columns), Views, Functions, and Procedures
--
-- Objects containing free-form SQL code can both depend on and be dependend on
-- by other such objects, via references to objects within the SQL code.  Thus
-- these objects must be created together in dependency order.  The algorithm
-- can be visualized as follows:
--
--     3    2    1    0     Priority
--
--     m <--,         e     Dependency graph
--          |    o <--,       (a <- b means b depends on a)
--     x <- y <- z <- q
--
--     Priority 0: Objects on which nothing depends: (e, q)
--     Priority 1: Objects the prior set depends on: (o, z)
--     Priority 2: Objects the prior set depends on: (y)
--     Priority 3: Objects the prior set depends on: (m, x)
--
--     Create the objects in order from greatest to least priority.
--

-- Discover potential chunk boundaries for tables.

DECLARE @column_depends TABLE
(
    object_id           int     NOT NULL,
    ordinal             int     NOT NULL,
    using_column_id     int     NOT NULL,
    used_column_id_max  int     NOT NULL,

    PRIMARY KEY         (object_id, ordinal),
    INDEX ux_1 UNIQUE   (object_id, using_column_id)
);

-- ... Potential chunk boundaries caused by indirectly self-referent computed columns.
WITH computed_column_depends AS
(
    SELECT
        depth            = 0,
        using_object_id  = o.object_id,
        using_column_id  = d.referencing_minor_id,
        used_object_name = r.quoted_name,
        used_object_id   = d.referenced_id,
        used_column_id   = ISNULL(d.referenced_minor_id, 0)
    FROM
        #objects o
    CROSS APPLY
        sys.dm_sql_referenced_entities(o.quoted_name, 'OBJECT') d
    INNER JOIN
        #objects r
        ON r.object_id = d.referenced_id
    WHERE 0=0
        AND o.type                 = 'U' -- in table
        AND d.referencing_minor_id > 0   -- referencing: column
        AND d.referenced_class     = 1   -- referenced:  object or column
  UNION ALL
    SELECT
        depth            = x.depth + 1,
        using_object_id  = x.using_object_id,
        using_column_id  = x.using_column_id,
        used_object_name = r.quoted_name,
        used_object_id   = d.referenced_id,
        used_column_id   = ISNULL(d.referenced_minor_id, 0)
    FROM
        computed_column_depends x
    CROSS APPLY
        sys.dm_sql_referenced_entities(x.used_object_name, 'OBJECT') d
    INNER JOIN
        #objects r
        ON r.object_id = d.referenced_id
    WHERE 0=0
        -- Stop if refs traced back to computed column's own table
        AND x.used_object_id != x.using_object_id
        --
        AND ISNULL(d.referencing_minor_id, 0) = x.used_column_id   -- referencing: object or column
        AND d.referenced_class                = 1                  -- referenced:  object or column
)
INSERT @column_depends
SELECT
    object_id          = using_object_id,
    ordinal            = DENSE_RANK() OVER (
                             PARTITION BY using_object_id
                             ORDER BY     using_column_id
                         ),
    using_column_id    = using_column_id,
    used_column_id_max = MAX(used_column_id)
FROM
    computed_column_depends
WHERE 0=0
    -- Only refs to a computed column's own table
    AND used_object_id = using_object_id
    -- Only refs that are indirect via some other object (e.g. a function)
    AND depth > 0
GROUP BY
    using_object_id,
    using_column_id
;

-- ... Potential chunk boundary at the end of every table.
--     Pretend there is a computed column just beyond the last column.
INSERT @column_depends
SELECT
    o.object_id,
    ordinal            = ISNULL(d.ordinal + 1, 1),
    using_column_id    = c.column_id + 1,
    used_column_id_max = c.column_id
FROM
    #objects o
CROSS APPLY
    (
        -- Max column_id
        SELECT TOP 1 c.column_id
        FROM sys.columns c
        WHERE c.object_id = o.object_id
        ORDER BY c.column_id DESC
    ) c
OUTER APPLY
    (
        -- Max ordinal
        SELECT TOP 1 d.ordinal
        FROM @column_depends d
        WHERE d.object_id = o.object_id
        ORDER BY d.ordinal DESC
    ) d
WHERE 0=0
    AND o.type = 'U' -- table
;

-- Discover tables split into as few chunks as possible

DECLARE @object_parts TABLE
(
    object_id       int             NOT NULL,
    column_id_min   int             NOT NULL,
    column_id_max   int             NOT NULL,
    type            char(4)         NOT NULL,
    schema_name     sysname         NOT NULL,
    object_name     sysname         NOT NULL,
    column_name     sysname             NULL,

    PRIMARY KEY (object_id, column_id_min)
);

WITH chunks AS
(
    SELECT
        object_id,
        ordinal,
        use_chunk     = 1,
        column_id_min = 0, -- 0 to catch refs to table but without column info
        column_id_max = using_column_id - 1
    FROM
        @column_depends
    WHERE
        ordinal = 1
  UNION ALL
    SELECT
        this.object_id,
        this.ordinal,
        use_chunk =
            CASE
                WHEN this.used_column_id_max > prev.column_id_max
                THEN 1 -- will accept this record
                ELSE 0 -- will ignore this record
            END,
        column_id_min =
            CASE
                WHEN this.used_column_id_max > prev.column_id_max
                THEN prev.column_id_max + 1     -- compute new value
                ELSE prev.column_id_min         -- keep at old value
            END,
        column_id_max =
            CASE
                WHEN this.used_column_id_max > prev.column_id_max
                THEN this.using_column_id - 1   -- compute new value
                ELSE prev.column_id_max         -- keep at old value
            END
    FROM
        chunks prev
    INNER JOIN
        @column_depends this
        ON  this.object_id = prev.object_id
        AND this.ordinal   = prev.ordinal + 1
)
INSERT @object_parts
SELECT
    object_id,
    column_id_min,
    column_id_max,
    type        = 'tabl',
    schema_name = OBJECT_SCHEMA_NAME (object_id),
    object_name = OBJECT_NAME        (object_id),
    column_name = COL_NAME           (object_id, column_id_min)
FROM
    chunks
WHERE
    use_chunk = 1
ORDER BY
    OBJECT_SCHEMA_NAME (object_id),
    OBJECT_NAME        (object_id),
    column_id_min
;

-- Discover views, functions, and procedures

INSERT @object_parts
SELECT
    object_id     = o.object_id,
    column_id_min = 0,
    column_id_max = 0,
    type          = CASE o.type
                        WHEN 'V'  THEN 'view'
                        WHEN 'P'  THEN 'proc'
                        WHEN 'FN' THEN 'func'
                        WHEN 'IF' THEN 'func'
                        WHEN 'TF' THEN 'func'
                        ELSE NULL
                    END,
    o.schema_name,
    o.object_name,
    column_name = NULL
FROM
    #objects o
WHERE 0=0
    AND o.type IN ('V', 'FN', 'IF', 'TF', 'P')
;

-- Discover the dependencies between tables, views, functions, and procedures.

DECLARE @depends TABLE
(
    using_object_id     int     NOT NULL,
    using_column_id     int     NOT NULL,
    used_object_id      int     NOT NULL,
    used_column_id      int     NOT NULL,

    PRIMARY KEY       (using_object_id, using_column_id,  used_object_id,  used_column_id),
    INDEX ux_1 UNIQUE (using_object_id, using_column_id,  used_object_id,  used_column_id) WHERE (using_column_id > 0),
    INDEX ux_2 UNIQUE ( used_object_id,  used_column_id, using_object_id, using_column_id)
);

INSERT @depends
SELECT DISTINCT
    using_object_id = o.object_id,
    using_column_id = o.column_id_min,
    used_object_id  = r.object_id,
    used_column_id  = r.column_id_min
FROM
    @object_parts o -- using
CROSS APPLY
    sys.dm_sql_referenced_entities(
        QUOTENAME(o.schema_name) + '.' + QUOTENAME(o.object_name),
        'OBJECT'
    ) d
INNER JOIN
    @object_parts r -- used
    ON  r.object_id      = d.referenced_id
    AND r.column_id_min <= d.referenced_minor_id
    AND r.column_id_max >= d.referenced_minor_id
WHERE 0=0
    AND d.referencing_minor_id >= o.column_id_min
    AND d.referencing_minor_id <= o.column_id_max
    AND d.referenced_class      = 1 -- object or column
    -- No direct self-references
    AND o.object_id != r.object_id
;

-- Compute dependency order and add appropriate DDL to the steps.

WITH prioritizing AS
(
    -- Lowest priority: objects on which nothing depends
    SELECT
        n = 0, o.*
    FROM
        @object_parts o
    WHERE
        NOT EXISTS (
            SELECT 0 FROM @depends d
            WHERE 0=0
                AND d.used_object_id = o.object_id
                AND d.used_column_id = o.column_id_min
        )
  UNION ALL
    -- Next higher: objects on which the prior set depends
    SELECT
        n = n + 1, o.*
    FROM
        prioritizing x
    INNER JOIN
        @depends d
        ON  d.using_object_id = x.object_id
        AND d.using_column_id = x.column_id_min
    INNER JOIN
        @object_parts o
        ON  o.object_id     = d.used_object_id
        AND o.column_id_min = d.used_column_id
)
, prioritized_objects AS
(
    SELECT
        priority = MAX(n),
        object_id, column_id_min, column_id_max,
        type, schema_name, object_name, column_name
    FROM prioritizing
    GROUP BY
        object_id, column_id_min, column_id_max,
        type, schema_name, object_name, column_name
)
INSERT #steps (kind, name, sql)
SELECT
    kind  = o.type,
    name  = o.schema_name + '.' + o.object_name + ISNULL('.' + o.column_name, ''),
    sql   = CASE
        WHEN type != 'tabl' THEN
            OBJECT_DEFINITION(o.object_id)
        ELSE
            CASE o.column_id_min
                WHEN 0 THEN
                    'CREATE TABLE ' + QUOTENAME(o.schema_name) + '.' + QUOTENAME(o.object_name) + @NL
                  + '(' + @NL
                ELSE
                    'ALTER TABLE ' + QUOTENAME(o.schema_name) + '.' + QUOTENAME(o.object_name) + ' ADD' +  @NL
            END
          + STUFF((
                SELECT ',' + @NL
                  + '    '
                  + QUOTENAME(c.name)
                  + REPLICATE(' ', 2 +
                        MAX(LEN(QUOTENAME(c.name))) OVER ()
                          - LEN(QUOTENAME(c.name))
                    )
                  + CASE c.is_computed
                        WHEN 0 THEN
                            ty.type
                          + REPLICATE(' ', 2 +
                                MAX(LEN(ty.type)) OVER ()
                                  - LEN(ty.type)
                            )
                          + ISNULL(' COLLATE ' + NULLIF(c.collation_name, @Collation), '')
                          + IIF(c.is_filestream = 1, ' FILESTREAM', '')
                          + IIF(c.is_sparse     = 1, ' SPARSE',     '')
                          + IIF(c.is_nullable   = 1, IIF(c.is_computed = 0, ' NULL', ''), ' NOT NULL')
                          + ISNULL(' IDENTITY('
                              + CONVERT(nvarchar(max), i.seed_value)      + ', '
                              + CONVERT(nvarchar(max), i.increment_value) + ')'
                              + IIF(i.is_not_for_replication = 1, ' NOT FOR REPLICATION', '')
                              , '')
                          + IIF(c.is_rowguidcol = 1, ' ROWGUIDCOL', '')
                            -- Not supported: MASKED WITH (FUNCTION = '...')
                            -- Not supported: GENERATED ALWAYS
                            -- Not supported: ENCRYPTED
                        ELSE
                            'AS ' + f.definition
                          + IIF(f.is_persisted = 1,                      ' PERSISTED', '')
                          + IIF(f.is_persisted = 1 AND f.is_nullable = 0, ' NOT NULL', '')
                    END
                FROM
                    sys.columns c
                INNER JOIN
                    @types ty
                    ON  ty.major_id = c.object_id
                    AND ty.minor_id = c.column_id
                LEFT JOIN
                    sys.identity_columns i
                    ON  i.object_id = c.object_id
                    AND i.column_id = c.column_id
                LEFT JOIN
                    sys.computed_columns f
                    ON  f.object_id = c.object_id
                    AND f.column_id = c.column_id
                WHERE 0=0
                    AND c.object_id     = o.object_id
                    AND c.column_id     BETWEEN o.column_id_min AND o.column_id_max
                    AND c.is_column_set = 0
                ORDER BY
                    c.column_id
                FOR XML
                    PATH(''), TYPE
            ).value('.', 'nvarchar(max)'), 1, 3, '') + @NL
          + CASE o.column_id_min
                WHEN 0 THEN
                    ');' + @NL
                ELSE
                    ';' + @NL
            END
    END
FROM
    prioritized_objects o
ORDER BY
    o.priority DESC,
    o.schema_name,
    CASE o.type
        WHEN 'tabl' THEN 0
        WHEN 'view' THEN 1
        ELSE             2
    END,
    o.object_name,
    o.column_id_min
;

-- -----------------------------------------------------------------------------
-- Table Data

DECLARE @Sql nvarchar(max);

WITH formats AS
(
    SELECT
        t.user_type_id,
        quoting = CASE ISNULL(b.name, t.name)
            WHEN 'char'             THEN 2 -- quoted/escaped
            WHEN 'nchar'            THEN 2 -- quoted/escaped
            WHEN 'varchar'          THEN 2 -- quoted/escaped
            WHEN 'nvarchar'         THEN 2 -- quoted/escaped
            WHEN 'uniqueidentifier' THEN 1 -- quoted
            WHEN 'datetime'         THEN 1 -- quoted
            WHEN 'smalldatetime'    THEN 1 -- quoted
            WHEN 'datetime2'        THEN 1 -- quoted
            WHEN 'datetimeoffset'   THEN 1 -- quoted
            WHEN 'date'             THEN 1 -- quoted
            WHEN 'time'             THEN 1 -- quoted
            WHEN 'geography'        THEN 1 -- quoted?
            WHEN 'geometry'         THEN 1 -- quoted?
            ELSE                         0 -- bare
        END,
        style = CASE ISNULL(b.name, t.name)
            WHEN 'binary'    THEN ', 1'
            WHEN 'varbinary' THEN ', 1'
            ELSE                  ''
        END
    FROM
        sys.types t -- nominal type, possibly user-defined
    LEFT JOIN
        sys.types b -- base type if t is user-defined
        ON  b.user_type_id = t.system_type_id
        AND 1              = t.is_user_defined
    WHERE 0=0
        AND t.is_table_type = 0
        AND t.name         != 'timestamp'
)
, tables_with_identity AS
(
    SELECT DISTINCT
        object_id
    FROM
        sys.columns
    WHERE
        is_identity = 1
)
SELECT @Sql =
'
    DECLARE
        @NL nchar(2) = CHAR(13) + CHAR(10),
        @QT nchar(1) = CHAR(39);
'
+
(
    SELECT '
        WITH step AS
        (
            SELECT
                kind = ''data'',
                name = ''' + REPLACE(o.display_name, '''', '''''') + ''',
                sql  =
                (
                    SELECT _
                    =
                        -- batch separator
                        CASE WHEN [<Export-Sql>$N] % 1000 = 1 AND [<Export-Sql>$N] > 1 THEN
                            -- at start of subsequent batch
                            ''GO'' + @NL +
                            ''''   + @NL
                        ELSE
                            ''''
                        END
                    '+
                        -- identity insert enable
                        CASE WHEN i.object_id IS NOT NULL THEN
                    '+
                            -- identity insert enable
                            CASE WHEN [<Export-Sql>$N] % 1000 = 1 THEN
                                -- at start of batch
                                ''SET IDENTITY_INSERT ' + REPLACE(o.quoted_name, '''', '''''') + ' ON;'' + @NL +
                                ''''                                                                     + @NL
                            ELSE
                                ''''
                            END
                    '
                        ELSE
                            ''
                        END
                    +'+
                        -- insert statement header or value tuple separator
                        CASE WHEN [<Export-Sql>$N] % 1000 = 1 THEN
                            -- at start of batch
                            ''INSERT ' + REPLACE(o.quoted_name, '''', '''''') + '''  + @NL +
                            ''    ( '                                                +
                                -- column names
                                STUFF((
                                    SELECT
                                        ', ' + REPLACE(QUOTENAME(c.name), '''', '''''')
                                    FROM
                                        sys.columns c
                                    INNER JOIN
                                        formats f
                                        ON f.user_type_id = c.system_type_id
                                    WHERE 0=0
                                        AND c.object_id   = o.object_id
                                        AND c.is_computed = 0
                                    ORDER BY
                                        c.column_id
                                    FOR XML
                                        PATH(''), TYPE
                                )
                                .value('.', 'nvarchar(max)')
                                , 1, 2, '')                                          +
                            ' )''                                                    + @NL +
                            ''VALUES''                                               + @NL
                        ELSE
                            '',''                                                    + @NL
                        END
                    +
                        -- value tuple
                        ''    ( '' +
                        '
                        +
                            -- values
                            STUFF((
                                SELECT _
                                  = ' + '', ''' + @NL
                                  + '              + '
                                  + 'ISNULL('
                                  +   IIF(f.quoting > 0, '@QT + ', '')
                                  +     IIF(f.quoting > 1, 'REPLACE(', '')
                                  +       'CONVERT(nvarchar(max), '
                                  +         QUOTENAME(c.name)
                                  +         f.style
                                  +       ')'
                                  +     IIF(f.quoting > 1, ', @QT, @QT+@QT)', '')
                                  +   IIF(f.quoting > 0, ' + @QT', '')
                                  + ', ''NULL'')'
                                FROM
                                    sys.columns c
                                INNER JOIN
                                    formats f
                                    ON f.user_type_id = c.system_type_id
                                WHERE 0=0
                                    AND c.object_id   = o.object_id
                                    AND c.is_computed = 0
                                ORDER BY
                                    c.column_id
                                FOR XML
                                    PATH(''), TYPE
                            )
                            .value('.', 'nvarchar(max)')
                            , 1, 7, '')
                        +
                        ' + '' )''
                    +
                        -- statement terminator
                        CASE WHEN [<Export-Sql>$N] % 1000 = 0 OR [<Export-Sql>$N] = [<Export-Sql>$C] THEN
                            '';'' + @NL
                        ELSE
                            ''''
                        END
                    '+
                        -- identity insert disable
                        CASE WHEN i.object_id IS NOT NULL THEN
                    '+
                            -- identity insert disable
                            CASE WHEN [<Export-Sql>$N] % 1000 = 0 OR [<Export-Sql>$N] = [<Export-Sql>$C] THEN
                                -- at end of batch
                                ''''                                                                      + @NL +
                                ''SET IDENTITY_INSERT ' + REPLACE(o.quoted_name, '''', '''''') + ' OFF;'' + @NL
                            ELSE
                                ''''
                            END
                    '
                        ELSE
                            ''
                        END
                    +'
                    FROM
                        (
                            SELECT *,
                                [<Export-Sql>$N] = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
                                [<Export-Sql>$C] = COUNT(*)     OVER ()
                            FROM ' + o.quoted_name + '
                        ) _
                    FOR XML
                        PATH(''''), TYPE
                )
                .value(''.'', ''nvarchar(max)'')
        )
        INSERT #steps (kind, name, sql)
        SELECT * FROM step
        WHERE sql IS NOT NULL;
    '
    FROM
        #objects o
    INNER JOIN
        sys.dm_db_partition_stats s
        ON  s.object_id = o.object_id
        AND s.index_id IN (0, 1) -- heap or clustered
    LEFT JOIN
        tables_with_identity i
        ON i.object_id = o.object_id
    WHERE 0=0
        AND o.type = 'U' -- table
        AND s.row_count > 0 -- not empty
        AND NOT EXISTS (
            SELECT NULL FROM #excluded_data x
            WHERE o.display_name LIKE x.pattern
        )
    ORDER BY
        o.schema_name,
        o.object_name
    FOR XML
        PATH(''), TYPE
)
.value('.', 'nvarchar(max)');

EXEC(@Sql);

-- -----------------------------------------------------------------------------
-- Primary/Unique Key Constraints and Indexes

INSERT #steps (kind, name, sql)
SELECT
    kind = CASE
        WHEN i.is_primary_key       = 1 THEN 'pk'
        WHEN i.is_unique_constraint = 1 THEN 'uq'
        WHEN i.is_unique            = 1 THEN 'ux'
        ELSE                                 'ix'
    END,
    i.name,
    sql =
    CASE
        WHEN i.is_primary_key = 1 THEN
            'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)    + @NL +
            '    ADD CONSTRAINT ' + QUOTENAME(i.name)                       + @NL +
            '    PRIMARY KEY'                                               +
            IIF(i.index_id > 1, ' NONCLUSTERED', '')                        +
            ' ('
        WHEN i.is_unique_constraint = 1 THEN
            'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)    + @NL +
            '    ADD CONSTRAINT ' + QUOTENAME(i.name)                       + @NL +
            '    UNIQUE'                                                    +
            IIF(i.index_id = 1, ' CLUSTERED', '')                           +
            ' ('
        ELSE
            'CREATE'                                                        +
            IIF(i.is_unique = 1, ' UNIQUE', '')                             +
            IIF(i.index_id  = 1, ' CLUSTERED', '')                          +
            ' INDEX ' + QUOTENAME(i.name)                                   + @NL +
            '    ON ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ' ('
    END
  + STUFF((
        SELECT ', '
          + QUOTENAME(c.name)
          + IIF(ic.is_descending_key = 1, ' DESC', '')
        FROM
            sys.index_columns ic
        INNER JOIN
            sys.columns c
            ON  c.object_id = ic.object_id
            AND c.column_id = ic.column_id
        WHERE 0=0
            AND ic.object_id = i.object_id
            AND ic.index_id  = i.index_id
            AND ic.is_included_column = 0
        ORDER BY
            ic.key_ordinal
        FOR XML
            PATH(''), TYPE
    ).value('.', 'nvarchar(max)'), 1, 2, '')
  + ')'
  + ISNULL(@NL
      + '    INCLUDE ('
      + STUFF((
            SELECT ', '
              + QUOTENAME(c.name)
            FROM
                sys.index_columns ic
            INNER JOIN
                sys.columns c
                ON  c.object_id = ic.object_id
                AND c.column_id = ic.column_id
            WHERE 0=0
                AND ic.object_id = i.object_id
                AND ic.index_id  = i.index_id
                AND ic.is_included_column = 1
            ORDER BY
                ic.key_ordinal
            FOR XML
                PATH(''), TYPE
        ).value('.', 'nvarchar(max)'), 1, 2, '')
      + ')'
      , ''
    )
  + @NL
  + ISNULL('    WHERE ' + i.filter_definition + @NL, '')
  + '    WITH ('
  +     'DATA_COMPRESSION = ' + ISNULL(p.data_compression_desc, 'NONE')
  +     IIF(i.fill_factor BETWEEN 1 AND 99, ', FILLFACTOR = ' + CONVERT(nvarchar, i.fill_factor), '')
  +     IIF(i.is_padded = 1,                ', PAD_INDEX = ON', '')
  + ');' + @NL
FROM
    @schemas s
INNER JOIN
    sys.tables t
    ON t.schema_id = s.schema_id
INNER JOIN
    sys.indexes i
    ON i.object_id = t.object_id
LEFT JOIN
    sys.partitions p
    ON  p.object_id        = i.object_id
    AND p.index_id         = i.index_id
    AND p.partition_number = 1
WHERE 0=0
    AND t.is_ms_shipped = 0
    AND t.is_external   = 0
    AND t.is_filetable  = 0
    AND i.type IN (1, 2) -- Clustered, Nonclustered
ORDER BY
    s.name, t.name, i.index_id
;

-- -----------------------------------------------------------------------------
-- Foreign Key Constraints

INSERT #steps (kind, name, sql)
SELECT
    'fk', s.name + '.' + t.name + '.' + k.name,
    sql =
    'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + @NL +
    '    ADD CONSTRAINT ' + QUOTENAME(k.name)                    + @NL +
    '    FOREIGN KEY (' +
        -- Concatenate the parent columns
        STUFF((
            SELECT ', ' + COL_NAME(kc.parent_object_id, kc.parent_column_id)
            FROM sys.foreign_key_columns kc
            WHERE kc.constraint_object_id = k.object_id
            ORDER BY kc.constraint_column_id
            FOR XML PATH(''), TYPE
        ).value('.', 'nvarchar(max)'), 1, 2, '') +
    ')' + @NL +
    '    REFERENCES ' + QUOTENAME(rs.name) + '.' + QUOTENAME(rt.name) + ' (' +
        -- Concatenate the referenced columns
        STUFF((
            SELECT ', ' + COL_NAME(kc.referenced_object_id, kc.referenced_column_id)
            FROM sys.foreign_key_columns kc
            WHERE kc.constraint_object_id = k.object_id
            ORDER BY kc.constraint_column_id
            FOR XML PATH(''), TYPE
        ).value('.', 'nvarchar(max)'), 1, 2, '') +
    ')' +
    ISNULL(CASE k.update_referential_action
        WHEN 1 THEN @NL + '    ON UPDATE CASCADE'
        WHEN 2 THEN @NL + '    ON UPDATE SET NULL'
        WHEN 3 THEN @NL + '    ON UPDATE SET DEFAULT'
        ELSE NULL
    END, '') +
    ISNULL(CASE k.delete_referential_action
        WHEN 1 THEN @NL + '    ON DELETE CASCADE'
        WHEN 2 THEN @NL + '    ON DELETE SET NULL'
        WHEN 3 THEN @NL + '    ON DELETE SET DEFAULT'
        ELSE NULL
    END, '') +
    ';' + @NL
FROM
    @schemas s
INNER JOIN
    sys.tables t
    ON t.schema_id = s.schema_id
INNER JOIN
    sys.foreign_keys k
    ON k.parent_object_id = t.object_id
INNER JOIN
    sys.tables rt
    ON rt.object_id = k.referenced_object_id
INNER JOIN
    @schemas rs
    ON rs.schema_id = rt.schema_id
WHERE 0=0
    AND t.is_ms_shipped = 0
    AND t.is_external   = 0
    AND t.is_filetable  = 0
    AND k.is_ms_shipped = 0
ORDER BY
    s.name, t.name, k.name
;

-- -----------------------------------------------------------------------------
-- Default Constraints

INSERT #steps (kind, name, sql)
SELECT
    'df', s.name + '.' + t.name + '.' + d.name,
    sql=
    'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + @NL +
    '    ADD ' +
    IIF(d.is_system_named = 0, 'CONSTRAINT ' + QUOTENAME(d.name) + @NL + '    ', '') +
    'DEFAULT ' + d.definition + ' FOR ' + QUOTENAME(c.name) + ';' + @NL
FROM
    @schemas s
INNER JOIN
    sys.tables t
    ON t.schema_id = s.schema_id
INNER JOIN
    sys.default_constraints d
    ON d.parent_object_id = t.object_id
INNER JOIN
    sys.columns c
    ON  c.object_id = d.parent_object_id
    AND c.column_id = d.parent_column_id
WHERE 0=0
    AND t.is_ms_shipped = 0
    AND t.is_external   = 0
    AND t.is_filetable  = 0
    AND d.is_ms_shipped = 0
ORDER BY
    s.name, t.name, d.name
;

-- -----------------------------------------------------------------------------
-- Check Constraints

INSERT #steps (kind, name, sql)
SELECT
    'ck', /*s.name + '.' + t.name + '.' +*/ c.name,
    sql =
    'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + @NL +
    '    ADD CONSTRAINT ' + QUOTENAME(c.name)                    + @NL +
    '    CHECK ' + c.definition + ';'                            + @NL
FROM
    @schemas s
INNER JOIN
    sys.tables t
    ON t.schema_id = s.schema_id
INNER JOIN
    sys.check_constraints c
    ON c.parent_object_id = t.object_id
WHERE 0=0
    AND t.is_ms_shipped = 0
    AND t.is_external   = 0
    AND t.is_filetable  = 0
    AND c.is_ms_shipped = 0
ORDER BY
    s.name, t.name, c.name
;

-- -----------------------------------------------------------------------------
-- Triggers (DML Only)

INSERT #steps (kind, name, sql)
SELECT
    'trig', s.name + '.' + r.name, OBJECT_DEFINITION(r.object_id)
FROM
    @schemas s
INNER JOIN
    sys.tables t
    ON t.schema_id = s.schema_id
INNER JOIN
    sys.triggers r
    ON  r.parent_class = 1 -- object
    AND r.parent_id    = t.object_id
WHERE 0=0
    AND t.is_ms_shipped = 0
    AND t.is_external   = 0
    AND t.is_filetable  = 0
    AND r.is_ms_shipped = 0
    AND r.type          = 'TR' -- exclude CLR triggers
    AND NOT EXISTS (SELECT 0 FROM #excluded_objects WHERE r.name LIKE pattern)

UNION ALL

SELECT
    'trig', s.name + '.' + r.name, OBJECT_DEFINITION(r.object_id)
FROM
    @schemas s
INNER JOIN
    sys.views v
    ON v.schema_id = s.schema_id
INNER JOIN
    sys.triggers r
    ON  r.parent_class = 1 -- object
    AND r.parent_id    = v.object_id
WHERE 0=0
    AND v.is_ms_shipped = 0
    AND r.is_ms_shipped = 0
    AND r.type          = 'TR' -- exclude CLR triggers
    AND NOT EXISTS (SELECT 0 FROM #excluded_objects WHERE r.name LIKE pattern)
;


-- -----------------------------------------------------------------------------
-- Extended Properties

WITH properties AS
(
    SELECT p.*,
        level0type = CASE class
            WHEN 1 THEN 'SCHEMA'
            ELSE NULL
        END,
        level0name = CASE class
            WHEN 1 THEN OBJECT_SCHEMA_NAME(major_id)
            ELSE NULL
        END,
        level1type = CASE class
            WHEN 1 THEN
                CASE o.type
                    WHEN 'U'  THEN 'TABLE'
                    WHEN 'TR' THEN 'TABLE'
                    ELSE NULL
                END
            ELSE NULL
        END,
        level1name = CASE class
            WHEN 1 THEN
                CASE o.type
                    WHEN 'U'  THEN OBJECT_NAME(major_id)
                    WHEN 'TR' THEN OBJECT_NAME(parent_object_id)
                    ELSE NULL
                END
            ELSE NULL
        END,
        level2type = CASE class
            WHEN 1 THEN
                CASE o.type
                    WHEN 'U'  THEN IIF(minor_id > 0, 'COLUMN', NULL)
                    WHEN 'TR' THEN 'TRIGGER'
                    ELSE NULL
                END
            ELSE NULL
        END,
        level2name = CASE class
            WHEN 1 THEN
                CASE o.type
                    WHEN 'U'  THEN COL_NAME(major_id, minor_id)
                    WHEN 'TR' THEN OBJECT_NAME(major_id)
                    ELSE NULL
                END
            ELSE NULL
        END
    FROM
        sys.extended_properties p
    LEFT JOIN
        sys.objects o
        ON  o.object_id = p.major_id
        AND p.class     = 1 -- object or column
    INNER JOIN
        #objects objs
        ON o.object_id = objs.object_id
)
, properties_as_string AS
(
    SELECT
        class, minor_id,
        level0type = '''' + REPLACE(level0type, '''', '''''') + '''',
        level0name = '''' + REPLACE(level0name, '''', '''''') + '''',
        level1type = '''' + REPLACE(level1type, '''', '''''') + '''',
        level1name = '''' + REPLACE(level1name, '''', '''''') + '''',
        level2type = '''' + REPLACE(level2type, '''', '''''') + '''',
        level2name = '''' + REPLACE(level2name, '''', '''''') + '''',
        name       = '''' + REPLACE(name,       '''', '''''') + '''',
        value_text =
        CASE
            WHEN value IS NULL THEN
                'NULL'
            WHEN SQL_VARIANT_PROPERTY(value, 'BaseType') IN (
                'nchar', 'nvarchar'
            ) THEN
                'N''' + REPLACE(CONVERT(nvarchar(max), value), '''', '''''') + ''''
            WHEN SQL_VARIANT_PROPERTY(value, 'BaseType') IN (
                'char', 'varchar', 'uniqueidentifier',
                'date', 'time', 'smalldatetime', 'datetime', 'datetime2', 'datetimeoffset'
            ) THEN
                '''' + REPLACE(CONVERT(nvarchar(max), value), '''', '''''') + ''''
            ELSE
                CONVERT(nvarchar(max), value)
        END
    FROM
        properties
)
, properties_sql AS
(
    SELECT sql =
    (
        SELECT
            'EXEC sys.sp_addextendedproperty ' + name + ', ' + value_text +
            ISNULL(', ' + level0type + ', ' + level0name, '') +
            ISNULL(', ' + level1type + ', ' + level1name, '') +
            ISNULL(', ' + level2type + ', ' + level2name, '') +
            ';' + @NL
        FROM
            properties_as_string
        ORDER BY
            class, level0type, level0name, level1type, level1name, minor_id
        FOR XML
            PATH(''), TYPE
    )
    .value('.', 'nvarchar(max)')
)
INSERT #steps (kind, name, sql)
SELECT
    'prop', '(all)', sql
FROM
    properties_sql
WHERE
    sql IS NOT NULL
;

-- -----------------------------------------------------------------------------
-- Permissions

INSERT #steps (kind, name, sql)
SELECT
    'perm', '(all)',
    sql =
    (
        SELECT
            g.state_desc + ' ' +
            STUFF(
            (
                -- Permissions
                SELECT ', ' + p.permission_name
                FROM sys.database_permissions p
                WHERE 0=0
                    AND p.class                = x.class
                    AND p.major_id             = x.major_id
                    AND p.minor_id             = 0
                    AND p.state                = g.state
                    AND p.grantee_principal_id = g.grantee_principal_id
                ORDER BY
                    p.permission_name
                FOR XML
                    PATH(''), TYPE
            )
            .value('.', 'nvarchar(max)'), 1, 2, '') +
            ' ON ' + x.qname + ' TO ' + QUOTENAME(u.name) + ';' + @NL
            COLLATE Latin1_General_100_CI_AS_SC
        FROM
            (
                -- Objects
                SELECT
                    class       = 1, -- object or column
                    major_id    = o.object_id,
                    schema_name = o.schema_name,
                    name        = o.object_name,
                    qname       = o.quoted_name
                FROM #objects o
              UNION ALL
                -- User-defined types
                SELECT
                    class       = 6, -- type
                    major_id    = t.user_type_id,
                    schema_name = s.name,
                    name        = t.name,
                    qname       = 'TYPE::' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
                FROM
                    @schemas s
                INNER JOIN
                    sys.types t
                    ON  t.schema_id        = s.schema_id
                    AND t.is_user_defined  = 1
                    AND t.is_assembly_type = 0
            ) x
        CROSS APPLY
            (
                -- Principals (users, roles, etc.)
                SELECT DISTINCT g.state, g.state_desc, g.grantee_principal_id
                FROM sys.database_permissions g
                WHERE 0=0
                    AND g.class    = x.class
                    AND g.major_id = x.major_id
                    AND g.minor_id = 0
            ) g
        INNER JOIN
            sys.database_principals u
            ON u.principal_id = g.grantee_principal_id
        ORDER BY
           x.class, x.schema_name, x.name, g.state, u.name
        FOR XML
            PATH(''), TYPE
    )
    .value('.', 'nvarchar(max)')
;

-- -----------------------------------------------------------------------------
-- Output

SELECT
    kind, name, sql
    = '-- ----------------------------------------------------------------------------' + @NL
    + 'PRINT ''+ ' + kind + ' ' + name + ''''                                           + @NL
    + 'GO'                                                                              + @NL
    + ''                                                                                + @NL
    + sql                                                  + IIF(RIGHT(sql, 2) = @NL, '', @NL)
    + 'GO'                                                                              + @NL
    + ''                                                                                + @NL
FROM
    #steps
ORDER BY
    step_id
;
