# athena2pd
[![](https://img.shields.io/badge/python-2.7+-blue.svg)](https://www.python.org/download/releases/2.7.0/)
[![](https://img.shields.io/badge/python-3.6+-blue.svg)](https://www.python.org/download/releases/3.6.0/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


athena2pd - Amazon Athena to Pandas Dataframe

## About

Useful tool to help simplify the access of databases stored in Amazon Athena by using SQL and pandas DataFrames. 

The end user simply needs to provide the query and the bucket where the results are stored, then this package will run the query and return a DataFrame with the data in it, ready to be used for whatever is desired.

## Installation
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install athena2pd.

```bash
pip install athena2pd
```

## Usage

Before use, you will need an AWS key pair, and an IAM profile set up to access both Amazon Athena and S3.

Using local .aws/ files is the safest way to connect, rather than providing the keys in the code. See the example files below:

The `.aws/credentials` file is set up like this:
```
[default]
aws_access_key_id = {access_key_id}
aws_secret_access_key = {secret_access_key}

...
```

In addition, the `.aws/config` is set up similar to this:
```
[default]
output = json
region = us-east-1

[profile athena-role]
role_arn = arn:aws:iam::{iam-id-number}:role/{role-name}
source_profile = default
region = us-east-1

...
```

Once that is set up, in your python code, the athena2pd package can be used like so:
```python
from athena2pd import AthenaDFConnector

# Initialize the AthenaDFConnector object
ath = AthenaDFConnector(aws_profile_name='athena-role')

# Example SQL query
sql_query = '''
SELECT COUNT(*) AS Count
FROM testcatalog.testdatabase.testtable
'''

# Example output location
output_loc = 's3://bucket-name/sub/folder'

# Query Athena and load into a pandas DataFrame
df = ath.query(query_string=sql_query, s3_output_location=output_loc)
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](LICENSE)