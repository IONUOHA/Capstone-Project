nycpayroll_2021_df
nycpayroll_2020_df
empmaster_df
TitleMaster_df
Agencymaster_df
# make a copy
df_2021 = nycpayroll_2021_df.copy()
df_2020 = nycpayroll_2020_df.copy()
df_emp = empmaster_df.copy()
df_title = TitleMaster_df.copy()
df_agency = Agencymaster_df.copy()
df_2021a = nycpayroll_2021_df.copy()
df_2021a = nycpayroll_2021_df.copy()
df_2021a.head()
df_2021a.tail()
df_2020.tail()
df_2020.head()
# structure of dataset
df_2020.info() # agencystartdate = object
df_2021a.info() # agencystartdate = object
df_agency.info()
df_title.info()
df_emp.info()
df_2020.isnull().sum()
df_2021a.isnull().sum()
df_agency.isnull().sum()
df_title.isnull().sum()
df_emp.isnull().sum()
df_title.isnull().sum() # only is null value dataset
#check for duplicates (no dups found in all datasets)
df_title.duplicated().sum()
df_2021a.describe()
df_2020.columns
df_2021a.columns
df_agency.columns
df_title.columns
df_emp.columns
# Drop duplicates
df_2020 = df_2020.drop_duplicates()
df_2021a = df_2021.drop_duplicates()
df_emp = df_emp.drop_duplicates()
df_title = df_title.drop_duplicates()
df_agency = df_agency.drop_duplicates()
# converting column types (date from Object to datetime)
df_2020.info()
df_2020['AgencyStartDate']
df_2020['AgencyStartDate'] = pd.to_datetime(df_2020['AgencyStartDate'], format='%m/%d/%Y')
df_2020.info()
df_2020
df_2021a['AgencyStartDate']
df_2021a['AgencyStartDate'] = pd.to_datetime(df_2021a['AgencyStartDate'], format='%m/%d/%Y')
df_2021a.info()
##change/clean column names
df_2021a.columns
df_2021a.columns.str.lower()
df_2021a.columns = df_2021a.columns.str.lower()
df_2021a.columns
df_2021a.head()
df_2020.columns
df_2020.columns.str.lower()
df_2020.columns = df_2020.columns.str.lower()
df_2020.columns
df_agency.columns.str.lower()
df_emp.columns.str.lower()
df_title.columns.str.lower()
df_agency.columns = df_agency.columns.str.lower()
df_emp.columns = df_emp.columns.str.lower()
df_title.columns = df_title.columns.str.lower()
# renaming columns
df_2021a.rename(columns={'leavestatusasofjune30':'june30_leavestatus','titledescription':'title_des','othours':'OT_hours','totalotpaid':'total_OT_paid','agencycode':'agencyid'},inplace=True)
df_2020.rename(columns={'leavestatusasofjune30':'june30_leavestatus','titledescription':'title_des','othours':'OT_hours','totalotpaid':'total_OT_paid'},inplace=True)
# Missing values
df_title.isnull().sum()
#dropping missing value
df_title.dropna()
df_title.dropna().info()
cleaned_df_2021a = df_2021a.copy()
cleaned_df_2020 = df_2020.copy()
cleaned_df_emp = df_emp.copy()
cleaned_df_title = df_title.copy()
cleaned_df_agency = df_agency.copy()
## add a new column for full name
df_2021a['full_name'] = df_2021a['lastname'] + ' ' + df_2021a['firstname']
df_2020['full_name'] = df_2020['lastname'] + ' ' + df_2020['firstname']
df_emp['full_name'] = df_emp['lastname'] + ' ' + df_emp['firstname']
df_2020.columns
df_emp
#Drop columns firstname and lastname
df_2021a.drop(columns=['firstname','lastname'],inplace=True)
df_2021a
df_2020.drop(columns=['firstname','lastname'],inplace=True)
df_emp.drop(columns=['firstname','lastname'],inplace=True)
df_emp.columns = ['employee_id','full_name']
df_agency.columns = ['agency_id','agency_name']
df_title.columns = ['title_code','job_title']
nyc_payroll_combined = pd.concat([df_2020, df_2021a], axis=0)
nyc_payroll_combined.reset_index(drop=True, inplace=True)
indices_to_drop = nyc_payroll_combined[nyc_payroll_combined['fiscalyear'] == 1998].index
nyc_payroll_combined = nyc_payroll_combined.drop(indices_to_drop)
indices_to_drop = nyc_payroll_combined[nyc_payroll_combined['fiscalyear'] == 1999].index
nyc_payroll_combined = nyc_payroll_combined.drop(indices_to_drop)
nyc_payroll_combined.rename(columns={'fiscalyear':'fiscal_year','payrollnumber':'payroll_number','agencyname':'agency_name','agencyid':'agency_id','total_OT_paid':'total_ot','employeeid':'employee_id','basesalary':'base_salary'},inplace=True)