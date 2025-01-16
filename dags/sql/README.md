# Table documentation

## Main schema(final product):

### `master`

### `inr_measure_final`

### `bop_measure_final`

### `entity_dimension_final`

## Intermediate tables:
### `balance_of_pay`

| column_name | data_type | meaning |
| --- | --- | --- |
| entity | bigint | The index of an entity(country or union) | 
| measure | bigint | The measure of balance of pay (goods, services or capital account) |
| date | timestamp without time zone | |
| value | double precision | |

### `interest_rate`

| column_name | data_type | meaning |
| --- | --- | --- |
| entity | bigint | The index of an entity(country or union) |
| meas | bigint | The measure of interest rate (short-term, immediate-term, long-term) |
| date | timestamp without time zone | |
| value | double precision | |


### `ex_rate`

| column_name | data_type | meaning |
| --- | --- | --- |
| currency | bigint | Index of the currency |
| time_period | timestamp without time zone | Date |
| rate | double precision | Exchange rate EUR/currency |


## Tables for joining
### `currencies`

| column_name | data_type | meaning |
| --- | --- | --- |
| currency_id | bigint | index of currency in `balance_of_pay` table |
| entity_id | bigint | index of the country in `balance_of_pay` table |

### `dim_currency`

| column_name | data_type | meaning |
| --- | --- | --- |
| name | text | The name of the currency(abbreviation) |
| bop_id | bigint | Index in the `balance_of_pay` table |
| ex_id | bigint | Index in the `ex_rates` table |


### `dim_entity`

| column_name | data_type | meaning |
| --- | --- | --- |
| name | text | Abreviation of the name of the entity(country or union) |
| index | bigint | New index(will be present in the `master` relation)|
| bop_id | bigint | Index from the `balance_of_pay` table |
| int_id | bigint | Index from the `interest_rate` table |


### 