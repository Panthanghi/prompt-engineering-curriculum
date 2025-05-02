


import csv

def infer_column_logic(col_name, data_type):
    col_lower = col_name.lower()
    if 'date' in col_lower or col_lower.endswith(('dt', 'dat', 'tim')) or 'time' in col_lower:
        return [
            f"{{{{ default_to_number('{col_name}') }}}} as {col_name}_ORIG",
            f"{{{{ string_to_date('{col_name}') }}}} as {col_name}"
        ]
    elif data_type.upper() in ['DECIMAL', 'NUMERIC', 'NUMBER']:
        return [f"{{{{ default_to_number('{col_name}') }}}} as {col_name}"]
    else:
        return [col_name]

def read_columns(csv_path):
    columns = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            columns.append((row['column_name'], row['data_type']))
    return columns

def read_unique_keys(csv_path):
    keys = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            keys.append(row['column_name'])
    return keys

def generate_model(table_name, columns, unique_keys):
    all_col_names = [col for col, _ in columns]

    for key in unique_keys:
        if key not in all_col_names:
            print(f"Warning: Key column '{key}' not found in data type list!")

    surrogate_key = f"{{{{ generate_surrogate_key([{', '.join([f\"'{k}'\" for k in unique_keys])}]) }}}}"
    transformed_cols = []
    for col, dtype in columns:
        transformed_cols.extend(infer_column_logic(col, dtype))

    col_list_str = ",\n        ".join(transformed_cols)
    final_select = ",\n".join([col.split(' as ')[-1] if ' as ' in col else col for col in transformed_cols])

    template = f"""
{{{{ 
    config(
        materialized='incremental',
        unique_key={unique_keys if len(unique_keys) > 1 else f"'{unique_keys[0]}'"},
        tags=["ods", "infopro", "scheduled-nightly"]
    ) 
}}}}

WITH SOURCE AS (
    SELECT *, 1 AS BATCH_KEY_ID FROM {{{{ source('STAGING','STG_IFP_{table_name}') }}}}
    {{% if is_incremental() %}}
    WHERE to_timestamp_tz(SUBSTRING(CAPXTIMESTAMP,1,17),'YYYYMMDDHHMISSFF') >= '{{{{ get_max_event_time('SYS_CDC_DTM') }}}}'
    {{% endif %}}
),
INS_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS INS_BATCH_ID, 1 AS BATCH_KEY_ID
),
TRANSFORMED AS (
    SELECT 
        {surrogate_key} as PK_ODS_IFP_{table_name}_ID,
        CURRENT_TIMESTAMP as SYS_CREATE_DTM,
        INS_BATCH_ID AS SYS_EXEC_ID,
        CURRENT_TIMESTAMP AS SYS_LAST_UPDATE_DTM,
        'I' AS SYS_ACTION_CD,
        'N' AS SYS_DEL_IND,
        'Y' AS SYS_VALID_IND,
        '' AS SYS_INVALID_DESC,
        to_timestamp_tz(SUBSTRING(CAPXTIMESTAMP,1,17),'YYYYMMDDHHMISSFF') AS SYS_CDC_DTM,
        CAPXUSER AS SYS_CDC_LIB,
        {col_list_str}
    FROM SOURCE
    LEFT JOIN INS_BATCH_ID USING (BATCH_KEY_ID)
),
DEDUPED AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY {', '.join(unique_keys)} ORDER BY SYS_CDC_DTM DESC) AS ROW_NUM FROM TRANSFORMED
),
FINAL AS (
    SELECT * FROM DEDUPED WHERE ROW_NUM = 1
)

SELECT 
PK_ODS_IFP_{table_name}_ID,
SYS_CREATE_DTM,
SYS_EXEC_ID,
SYS_LAST_UPDATE_DTM,
SYS_ACTION_CD,
SYS_DEL_IND,
SYS_VALID_IND,
SYS_INVALID_DESC,
SYS_CDC_DTM,
SYS_CDC_LIB,
{final_select}
FROM FINAL
""".strip()
    return template

# Example usage:
columns = read_columns('/mnt/data/bipsd.csv')
unique_keys = read_unique_keys('/mnt/data/bipsd_keys.csv')
sql_code = generate_model('BIPSD', columns, unique_keys)

with open('/mnt/data/BIPSD_dbt_model.sql', 'w') as f:
    f.write(sql_code)

print("DBT model generated and saved to BIPSD_dbt_model.sql.")


-------


CREATE OR REPLACE VIEW split_fixed_segments AS
SELECT
  input_column,
  SUBSTRING(input_column, 1, INSTR(input_column, '|') - 1) AS segment1,
  SUBSTRING(input_column, INSTR(input_column, '|') + 1, 
            INSTR(input_column, '|', INSTR(input_column, '|') + 1) - INSTR(input_column, '|') - 1) AS segment2,
  SUBSTRING(input_column, INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1, 
            INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) - 
            INSTR(input_column, '|', INSTR(input_column, '|') + 1) - 1) AS segment3,
  SUBSTRING(input_column, INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1, 
            INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) - 
            INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) - 1) AS segment4,
  SUBSTRING(input_column, INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1, 
            INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1) - 
            INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) - 1) AS segment5,
  SUBSTRING(input_column, INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1) + 1, 
            LENGTH(input_column) - INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1)) AS segment6
FROM your_table;

SELECT 
  segment5 AS fifth_segment,
  segment6 AS sixth_segment
FROM split_fixed_segments;








Approach 1: Chained INSTR with SUBSTRING (Optimized for 5th and 6th Segments)
This extends the chained INSTR approach to extract both segments in a single query by computing the 4th and 5th delimiter positions and the string’s end.

VQL Query
vql



SELECT
  SUBSTRING(
    input_column,
    INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1,  -- After 4th delimiter
    INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1) - 
    INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) - 1
  ) AS fifth_segment,
  SUBSTRING(
    input_column,
    INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1) + 1,  -- After 5th delimiter
    LENGTH(input_column) - INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|', INSTR(input_column, '|') + 1) + 1) + 1) + 1)
  ) AS sixth_segment
FROM your_table;
Explanation:

5th Segment:
Start: After 4th delimiter (delimiter4 + 1, e.g., 18 in "123|abc|45 6|xyz |3aq|a56").
Length: Between 4th and 5th delimiters (delimiter5 - delimiter4 - 1, e.g., 21 - 17 - 1 = 3).
Result: "3aq".
6th Segment:
Start: After 5th delimiter (delimiter5 + 1, e.g., 22).
Length: From 5th delimiter to end (LENGTH - delimiter5, e.g., 23 - 21 = 2).
Result: "a56".
Empty Segments: Handles cases like "|||3aq||" (5th: "3aq", 6th: "").
Results:

"123|abc|45 6|xyz |3aq|a56" → 5th: "3aq", 6th: "a56".
"123|abc||xyz |3aq|" → 5th: "3aq", 6th: "".
"|||3aq||" → 5th: "3aq", 6th: "".



TRIM(SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
    POSITION('|' IN SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
        POSITION('|' IN SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
            POSITION('|' IN SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
                POSITION('|' IN "bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8") + 1)) + 1)) + 1)) + 1,
    POSITION('|' IN SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
        POSITION('|' IN SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
            POSITION('|' IN SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
                POSITION('|' IN "bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8") + 1)) + 1)) + 1)) - 1))





segment 2 : TRIM(SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
    POSITION('|' IN "bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8") + 1,
    POSITION('|' IN SUBSTRING("bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8",
        POSITION('|' IN "bv_CDW_ODS_OFC_Ra_Customer_Trx_Lines_All_clean"."Attribute_8") + 1)) - 1))



To effectively join the specified Oracle Receivables tables with RA_CUSTOMER_TRX_ALL as the driving table, it's essential to understand the relationships and appropriate join conditions between these tables. Here's a detailed breakdown:

1. RA_CUSTOMER_TRX_ALL and RA_CUSTOMER_TRX_LINES_ALL

Relationship: RA_CUSTOMER_TRX_ALL stores transaction header information, while RA_CUSTOMER_TRX_LINES_ALL contains the corresponding line details.

Join Condition: The tables are linked via the CUSTOMER_TRX_ID column.

Join Type: Use an INNER JOIN to retrieve transactions with associated line items.

Considerations: An INNER JOIN ensures only transactions with line items are included, preventing Cartesian products.


2. RA_CUSTOMER_TRX_ALL and AR_PAYMENT_SCHEDULES_ALL

Relationship: AR_PAYMENT_SCHEDULES_ALL maintains payment schedules for transactions.

Join Condition: Link using the CUSTOMER_TRX_ID column.

Join Type: An INNER JOIN is appropriate to fetch transactions with payment schedules.

Considerations: This join ensures only transactions with payment schedules are retrieved.


3. AR_PAYMENT_SCHEDULES_ALL and AR_CASH_RECEIPTS_ALL

Relationship: AR_CASH_RECEIPTS_ALL records details of cash receipts.

Join Condition: Connect using the PAYMENT_SCHEDULE_ID from AR_PAYMENT_SCHEDULES_ALL and CASH_RECEIPT_ID from AR_CASH_RECEIPTS_ALL.

Join Type: Use a LEFT JOIN to include all payment schedules, with or without associated cash receipts.

Considerations: A LEFT JOIN ensures that all payment schedules are included, even if there's no corresponding cash receipt, avoiding exclusion of transactions without payments.


4. RA_CUSTOMER_TRX_ALL and RA_CUST_TRX_LINE_GL_DIST_ALL

Relationship: RA_CUST_TRX_LINE_GL_DIST_ALL holds accounting distributions for transaction lines.

Join Condition: Join using CUSTOMER_TRX_ID and CUSTOMER_TRX_LINE_ID to ensure each line's distribution is accurately linked.

Join Type: An INNER JOIN is suitable to fetch transactions with their accounting distributions.

Considerations: This join focuses on transactions that have associated accounting entries, ensuring data integrity.


5. RA_CUSTOMER_TRX_ALL and GL_PERIODS

Relationship: GL_PERIODS contains information about accounting periods.

Join Condition: Link RA_CUSTOMER_TRX_ALL.GL_DATE with GL_PERIODS.END_DATE to determine the accounting period of each transaction.

Join Type: A LEFT JOIN is advisable to include all transactions, even if they don't fall within a defined accounting period.

Considerations: This join helps in identifying the accounting period for each transaction, which is crucial for financial reporting.


Comprehensive SQL Query

Combining these joins, the following SQL query retrieves detailed transaction information:

SELECT
    trx.TRX_NUMBER,
    trx.TRX_DATE,
    line.LINE_NUMBER,
    line.DESCRIPTION,
    ps.DUE_DATE,
    ps.AMOUNT_DUE_REMAINING,
    cr.RECEIPT_NUMBER,
    cr.RECEIPT_DATE,
    dist.ACCOUNT_CLASS,
    gl.PERIOD_NAME
FROM
    RA_CUSTOMER_TRX_ALL trx
INNER JOIN
    RA_CUSTOMER_TRX_LINES_ALL line
    ON trx.CUSTOMER_TRX_ID = line.CUSTOMER_TRX_ID
INNER JOIN
    AR_PAYMENT_SCHEDULES_ALL ps
    ON trx.CUSTOMER_TRX_ID = ps.CUSTOMER_TRX_ID
LEFT JOIN
    AR_RECEIVABLE_APPLICATIONS_ALL app
    ON ps.PAYMENT_SCHEDULE_ID = app.PAYMENT_SCHEDULE_ID
LEFT JOIN
    AR_CASH_RECEIPTS_ALL cr
    ON app.CASH_RECEIPT_ID = cr.CASH_RECEIPT_ID
INNER JOIN
    RA_CUST_TRX_LINE_GL_DIST_ALL dist
    ON line.CUSTOMER_TRX_LINE_ID = dist.CUSTOMER_TRX_LINE_ID
LEFT JOIN
    GL_PERIODS gl
    ON trx.GL_DATE BETWEEN gl.START_DATE AND gl.END_DATE
WHERE
    gl.PERIOD_SET_NAME = 'Your_Ledger_Period_Set_Name'
    AND trx.TRX_DATE BETWEEN TO_DATE('YYYY-MM-DD', 'YYYY-MM-DD') AND TO_DATE('YYYY-MM-DD', 'YYYY-MM-DD');

Notes:

Replace 'Your_Ledger_Period_Set_Name' with the actual period set name relevant to your ledger.

Adjust the date range in the WHERE clause as needed.

Ensure that all necessary indexes are in place to optimize query performance.


By structuring the joins as outlined, this query effectively retrieves comprehensive transaction data while minimizing the risk of Cartesian products or unintended duplicates.






_---------
To join `RA_CUSTOMER_TRX_ALL` (as the main table) with `HZ_CUST_ACCOUNTS`, `RA_CUSTOMER_TRX_LINES_ALL`, `HZ_CUST_SITE_USES_ALL`, `AR_PAYMENT_SCHEDULES_ALL`, `GL_PERIODS`, and `AR_CASH_RECEIPTS_ALL`, we need to establish the relationships between these tables based on Oracle Fusion’s Receivables (AR) and Trading Community Architecture (TCA) data models. Below, I’ll outline the join conditions, explain their purpose, and recommend join types with a focus on performance optimization.

---

### Table Relationships and Join Conditions

1. **`RA_CUSTOMER_TRX_ALL` to `HZ_CUST_ACCOUNTS`**:
   - **Join Condition**: `RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID`
   - **Purpose**: Links transactions to their bill-to customer accounts.
   - **Cardinality**: Many-to-1 (multiple transactions per customer account).
   - **Join Type**: `INNER JOIN` (assumes all transactions should have a valid customer).

2. **`RA_CUSTOMER_TRX_ALL` to `RA_CUSTOMER_TRX_LINES_ALL`**:
   - **Join Condition**: `RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_ID`
   - **Purpose**: Connects transaction headers to their lines.
   - **Cardinality**: 1-to-many (one transaction can have multiple lines).
   - **Join Type**: `INNER JOIN` (assumes transactions should have lines for meaningful data).

3. **`RA_CUSTOMER_TRX_ALL` to `HZ_CUST_SITE_USES_ALL`**:
   - **Join Condition**: `RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID = HZ_CUST_SITE_USES_ALL.SITE_USE_ID`
   - **Purpose**: Links transactions to the specific bill-to site use (e.g., billing address or purpose) of the customer. `HZ_CUST_SITE_USES_ALL` stores site usage details tied to customer accounts via `HZ_CUST_ACCT_SITES_ALL`, but the direct link from transactions uses `SITE_USE_ID`.
   - **Cardinality**: Many-to-1 (multiple transactions can reference the same site use).
   - **Join Type**: `INNER JOIN` (assumes transactions should have a valid bill-to site).

4. **`RA_CUSTOMER_TRX_ALL` to `AR_PAYMENT_SCHEDULES_ALL`**:
   - **Join Condition**: `RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID`
   - **Purpose**: Connects transactions to their payment schedules (e.g., due dates and amounts). `AR_PAYMENT_SCHEDULES_ALL` tracks the payment terms and status for each transaction.
   - **Cardinality**: 1-to-many (one transaction can have multiple payment schedules, e.g., for installments, though typically it’s 1-to-1 for simple invoices).
   - **Join Type**: `INNER JOIN` (assumes you’re interested in transactions with payment schedules).

5. **`RA_CUSTOMER_TRX_ALL` to `GL_PERIODS`**:
   - **Join Condition**: 
     ```sql
     RA_CUSTOMER_TRX_ALL.GL_DATE BETWEEN GL_PERIODS.START_DATE AND GL_PERIODS.END_DATE
     AND GL_PERIODS.PERIOD_SET_NAME = '<Your Period Set Name>'
     AND GL_PERIODS.ADJUSTMENT_PERIOD_FLAG = 'N'
     ```
   - **Purpose**: Links the transaction’s General Ledger posting date (`GL_DATE`) to the corresponding accounting period in `GL_PERIODS`. You’ll need to specify the `PERIOD_SET_NAME` (e.g., 'Vision Calendar') based on your setup.
   - **Cardinality**: Many-to-1 (multiple transactions can fall within the same GL period).
   - **Join Type**: `INNER JOIN` (assumes transactions should map to a valid GL period for financial reporting).

6. **`RA_CUSTOMER_TRX_ALL` to `AR_CASH_RECEIPTS_ALL`**:
   - **Join Condition**: Typically, there’s no direct join between these tables because cash receipts are applied to transactions via `AR_RECEIVABLE_APPLICATIONS_ALL`. However, if you’re looking for a direct link (e.g., via payment schedules or applications), an intermediate table is needed. A common approach is:
     - Use `AR_PAYMENT_SCHEDULES_ALL` as a bridge:
       ```sql
       AR_PAYMENT_SCHEDULES_ALL.CASH_RECEIPT_ID = AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID
       ```
     - Full condition from `RA_CUSTOMER_TRX_ALL`:
       ```sql
       RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID
       AND AR_PAYMENT_SCHEDULES_ALL.CASH_RECEIPT_ID = AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID
       ```
   - **Purpose**: Links transactions to cash receipts applied against them (e.g., payments received).
   - **Cardinality**: 1-to-many (one transaction can have multiple cash receipts applied over time).
   - **Join Type**: `LEFT JOIN` (since not all transactions may have cash receipts applied yet, e.g., open invoices).

---

### Performance Optimization
To ensure the best performance:
1. **Indexes**: Ensure the following columns are indexed:
   - `RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID`, `BILL_TO_CUSTOMER_ID`, `BILL_TO_SITE_USE_ID`, `GL_DATE`
   - `HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID`
   - `RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_ID`
   - `HZ_CUST_SITE_USES_ALL.SITE_USE_ID`
   - `AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID`, `CASH_RECEIPT_ID`
   - `GL_PERIODS.START_DATE`, `END_DATE`, `PERIOD_SET_NAME`
   - `AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID`

2. **Join Order**: Start with `RA_CUSTOMER_TRX_ALL` (main table) and join to smaller or more restrictive tables first (e.g., `HZ_CUST_ACCOUNTS`, `HZ_CUST_SITE_USES_ALL`) before joining to potentially larger tables like `RA_CUSTOMER_TRX_LINES_ALL` or `AR_PAYMENT_SCHEDULES_ALL`.

3. **Filter Early**: Apply `WHERE` clauses (e.g., on `RA_CUSTOMER_TRX_ALL.TRX_DATE` or `GL_DATE`) before joining to reduce the dataset.

4. **Join Types**: Use `INNER JOIN` for mandatory relationships (e.g., customer, lines, site uses, payment schedules, GL periods) and `LEFT JOIN` for optional relationships (e.g., cash receipts).

---

### Example Query
Here’s a comprehensive query with all tables:

```sql
SELECT 
    rct.trx_number,
    rct.trx_date,
    hca.account_number,
    hca.account_name,
    rctl.line_number,
    rctl.description,
    hcsu.site_use_code,
    aps.due_date,
    glp.period_name,
    acr.receipt_number,
    acr.amount
FROM 
    RA_CUSTOMER_TRX_ALL rct
INNER JOIN 
    HZ_CUST_ACCOUNTS hca
ON 
    rct.bill_to_customer_id = hca.cust_account_id
INNER JOIN 
    RA_CUSTOMER_TRX_LINES_ALL rctl
ON 
    rct.customer_trx_id = rctl.customer_trx_id
INNER JOIN 
    HZ_CUST_SITE_USES_ALL hcsu
ON 
    rct.bill_to_site_use_id = hcsu.site_use_id
INNER JOIN 
    AR_PAYMENT_SCHEDULES_ALL aps
ON 
    rct.customer_trx_id = aps.customer_trx_id
INNER JOIN 
    GL_PERIODS glp
ON 
    rct.gl_date BETWEEN glp.start_date AND glp.end_date
    AND glp.period_set_name = 'Vision Calendar' -- Replace with your period set
    AND glp.adjustment_period_flag = 'N'
LEFT JOIN 
    AR_CASH_RECEIPTS_ALL acr
ON 
    aps.cash_receipt_id = acr.cash_receipt_id
WHERE 
    rct.trx_date >= SYSDATE - 365
    AND rct.org_id = 123; -- Replace with your org ID
```

---

### Explanation of Choices
- **`INNER JOIN` for most tables**: Ensures only complete, valid data is returned (e.g., transactions with customers, lines, sites, schedules, and GL periods).
- **`LEFT JOIN` for `AR_CASH_RECEIPTS_ALL`**: Allows inclusion of transactions without payments (e.g., unpaid invoices).
- **GL Periods**: The range join on `GL_DATE` is optimized by filtering non-adjustment periods and specifying the period set.
- **Org Context**: Added `ORG_ID` filter as an example; adjust based on your multi-org setup.

Let me know if you need further refinements or specific performance tuning tips!




# 30-Day Prompt Engineering Curriculum

A comprehensive curriculum to master prompt engineering in 30 days.

## Course Overview
This curriculum is designed for aspiring prompt engineers who want to:
- Master prompt engineering fundamentals
- Build a professional portfolio
- Prepare for industry roles
- Develop practical skills

## Getting Started
1. Fork this repository
2. Follow the daily curriculum in the `/curriculum` directory
3. Complete projects in the `/projects` directory
4. Use resources in the `/resources` directory
5. Prepare for jobs using the `/career` directory

## Repository Structure
- `/curriculum`: Daily lessons and exercises
- `/projects`: Hands-on projects
- `/resources`: Learning materials and tools
- `/career`: Career preparation materials
- `/code`: Code examples and utilities

## Prerequisites
- Basic Python knowledge
- GitHub account
- Free accounts on:
  - Anthropic Claude
  - OpenAI
  - Hugging Face
  - Google Colab

## How to Use This Repository
1. **Daily Learning**
   - Navigate to current day in `/curriculum`
   - Read the day's README
   - Complete exercises
   - Check solutions
   - Document progress

2. **Projects**
   - Start projects after completing related modules
   - Follow project README instructions
   - Submit solutions in your fork

3. **Career Preparation**
   - Use materials in `/career`
   - Build portfolio
   - Practice interviews

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License
This project is licensed under the MIT License - see [LICENSE](LICENSE) file.
