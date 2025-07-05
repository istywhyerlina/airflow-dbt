
## Column Mapping

### Dim Company
source : Company

target : table dim_company
| Source Column   | Target Column   |Transformation |
|---------------- |---------------- |---------------|
| uuid generated  | `company_id`    |generated  uuid|
| `office_id`     | `nk_company` |----rename-----|
| `object_id`     | `object_id`     |---------------|
| `description`   | `description`   |---------------|
| `region`        | `region`        |---------------|
| `address1`      | `address1`      |---------------|
| `address2`      | `address2`      |---------------|
| `city`          | `city`          |---------------|
| `zip_code`      | `zip_code`      |---------------|
| `state_code`    | `state_code`    |---------------|
| `country_code`  | `country_code`  |---------------|
| `latitude`      | `latitude`      |---------------|
| `longitude`     | `longitude`     |---------------|
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |



### Dim People
source : file people

target : table dim_people

| Source Column                 | Target Column      | Transformation                                                   |
|--------------------------------|-------------------|-----------------------------------------------------------------|
| uuid generated             | `people_id`                     | -                                                     |
| `people_id`            | `nk_people`                  | rename                                                     |
| `first_name`            | `first_name`                  | -                                                     |
| `last_name`            | `last_name`                  | -                                                     |
| `birthplace`            | `birthplace`                  | -                                                     |
| `affiliation_name`            | `affiliation_name`                  | -                                                     |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |

### Dim Acquisition Table
source : table acquisition, dim_company, dim_date

target : table fct_acquisition
| Source Column              | Target Column                | Transformation                                      |
|----------------------------|----------------------------- |---------------------------------------------------- |
| uuid generated             | `acquisition_id`             | -                                                   |
| `acquisition_id`           | `acquisition_nk_id`          | -                                                   |
| `acquiring_object_id`,`company_id`| `acquiring_object_id`  | lookup to company_id based on acquiring_object_id  |
| `acquired_object_id` ,`company_id`| `acquired_object_id`   | lookup to company_id based on acquired_object_id   |
| `term_code`         | `term_code`         |          |
| `"price_amount"`                  | `"price_amount"`       | -                                                  |
| `prince_currency_code`            | `prince_currency_code` | -                                                  |
| `acquired_at`, `date_id`          | `acquired_at`          | lookup to date_id based on acquired_at             |-                                                  |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |

### Dim Funds Table
source : table funds, dim_date, dim_company

target : table dim_fund
| Source Column              | Target Column                | Transformation                                      |
|----------------------------|----------------------------- |---------------------------------------------------- |
| uuid generated             | `fund_id`             | -                                                   |
| `fund_id`                  | `nk_fund`          | -                                                   |
| `object_id`                | `object_id`           | lookup to company_id based on object_id             |
| `"name"`                   | `"name"`              | -                                                  |
| `funded_at`, `date_id`     | `funded_at`           | lookup to date_id based on funded_at               |
| `source_url`               | `source_url`          | -                                                  |
| `"raise_amount"`           | `"raise_amount"`      | -                                                  |
| `raise_currency_code`      | `raise_currency_code` | -                                                  |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |


### Dim Funding Rounds Table
source : table funding_rounds, dim_company
target : table dim_funding_round
| Source Column              | Target Column                | Transformation                                      |
|----------------------------|----------------------------- |---------------------------------------------------- |
| uuid generated             | `funding_round_id`               | -                                                     |
| `funding_round_id`         | `nk_funding_round`            | -                                                     |
| `object_id`,`company_id`   | `object_id`                      | lookup to company_id based on object_id               |
| `funded_at`, `date_id`     | `funded_at`                      | lookup to date_id based on funded_at                  |
| `"funding_round_type"`     | `"funding_round_type"`           | -                                                     |
| `funding_round_code`       | `funding_round_code`             | -                                                     |
| `raised_amount_usd`        | `raised_amount_usd`              | - |
| `raised_amount`            | `raised_amount`                  | - |
| `"raised_currency_code"`   | `"raised_currency_code"`         | - |
| `"pre_money_valuation_usd"`| `"pre_money_valuation_usd"`      | - |
| `post_money_valuation_usd` | `post_money_valuation_usd`       | - |
| `participants`             | `participants`                   | - |
| `is_first_round`           | `is_first_round`                 | - |
| `is_last_round`            | `is_last_round`                  | - |
| `created_by`               | `created_by`                     | -                                                  |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |


### Dim Ipos Table
source : table ipos, dim_company, dim_stock_symbol
target : table dim_ipos
| Source Column              | Target Column                | Transformation                                      |
|----------------------------|----------------------------- |---------------------------------------------------- |
| uuid generated             | `ipo_id`                     | -                                                     |
| `ipo_id`                   | `nk_ipo`                  | -                                                     |
| `object_id`,`company_id`   | `object_id`                  | lookup to company_id based on object_id               |
| `"valuation_amount"`       | `"valuation_amount"`         | -                                                     |
| `valuation_currency_code`  | `valuation_currency_code`    | -                                  |
| `raised_amount`            | `raised_amount`              | - |
| `raised_currency_code`     | `raised_currency_code`       | - |
| `public_at`, `date_id`     | `public_at`                  | lookup to date_id based on public_at                  |
| `stock_symbol`| `stock_symbol`          |          |
| `source_url`               | `source_url`                     | -                                                  |
| `source_description`       | `source_description`             | -                                                  |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |


### Fact Investments Table
source : file investments, dim_company, fct_ipos

target : table fct_investments

| Source Column                 | Target Column      | Transformation                                                   |
|--------------------------------|-------------------|-----------------------------------------------------------------|
| uuid generated             | `investment_id`                     | -                                                     |
| `investment_id`            | `nk_investment`                  | -                                                     |
| `funding_round_id`, `funding_round_id`| `funding_round_id`|lookup to funding_round_id in fct_funding_rounds based on funding_round_nk_id|
| `funded_object_id` , `company_id`            | `funded_object_id` | lookup to company_id in dim_company based on company_nk_id |
| `investor_object_id` , `company_id`            | `investor_object_id` | lookup to company_id in dim_company based on company_nk_id |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |


### Fact Person_Relationship Table
source : file relationship, dim_people, people (staging), dim_company

target : table fct_person_relationship

| Source Column                 | Target Column      | Transformation                                                   |
|--------------------------------|-------------------|-----------------------------------------------------------------|
| uuid generated             | `person_relationship_id`                     | -                                                     |
| `relationship_id`            | `nk_relationship`                  | -                                                     |
| `person_id`            | `person_id`                  | -                                                     |
| `person_id`, `person_object_id`| `person_id`|lookup to person_id in fct_people based on person_object_id|
| `relationship_object_id` , `company_id`            | `relationship_object_id` | lookup to company_id in dim_company based on company_nk_id |
| `start_at`, `date_id`          | `start_at`          | lookup to date_id based on start_at             |
| `end_at`, `date_id`          | `end_at`          | lookup to date_id based on end_at             |
| `is_past`            | `is_past`                  | Convert To Boolean                                                    |
`sequence`                  | -                                             |
`title`                  | -                                                 |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |



### Fact Company_Performance Table
source : file dim_company, fact_investment, dim_fund, dim_acquisition

target : table fct_person_relationship

| Source Column                 | Target Column      | Transformation                                                   |
|--------------------------------|-------------------|-----------------------------------------------------------------|
| uuid generated             | `performance_id`                     | -                                                     |
| `company_id`            | `company_id`                  | lookup to company_id in dim_company                                                     |
| fact_investment[`funded_company_id`]            | `total_invested_company`                  | count funded_company_id group by company_id                                                      |
| dim_fund[`fund_id`]            | `total_fund_raised_by_company`                  | count funded_company_id group by company_id                                                      |
| dim_acquisition[`acquired_object_id`]            | `total_company_acquired`                  | count funded_company_id group by company_id                                                      |
| `created_at`               | `created_at`                     |  generated as now                                                  |
| `updated_at`               | `updated_at`                     | generated as now                                                  |