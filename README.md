# Steps to RUN

1) Setup database in SQL:
```sql
create database neobase;
use neobase;
```
```bash
mysql -u root -p neobase < dump.sql
```
2) Setup the secrets folder.
    * Create a `.streamlit` folder in your main directory 
    * Inside `.streamlit` create file named `secrets.toml`
    * The fields to be included are as follows:
        * birdeye_api
        * telegram_api
        * telegram_hash
        * phone_no -> for the associated telegram account.
        * sql_password
3) Install requirements
```bash
pip install -r requirements.txt
```
4) Execute
```bash
streamlit run dashboard.py
```
