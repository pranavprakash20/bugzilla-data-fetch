import bugzilla
import psycopg
from datetime import datetime


class FetchCustomerData:

    def __init__(self, url, api_key, db_name, db_user, db_password, db_host, db_port, table_name):
        self.api_key = api_key
        self.url = url
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.bz_api = self._initialise_api_req()
        self.table = table_name
        self.db_conn = None

    def fetch_customer_bugs(self):
        """
        Master method which fetches the customer bugs in last 24 hrs. The same is appened to the database.
        Also every existing bz is checked for any changes in the parameters saved
        """
        # Before entering latest bugs, check if the existing bugs have field changes
        self._verify_existing_bug_fields()

        # Now append the data to the db
        URL = "QUERY URL"
        query = self.bz_api.url_to_query(URL)
        query["include_fields"] = ["id", "summary"]
        bugs = self.bz_api.query(query)
        print(f"Bugs reported in last 24 hours : {len(bugs)} ")
        self._append_data_to_db(bugs)
        self.db_conn.close()

    def _verify_existing_bug_fields(self):
        """
        Verify whether the existing bugs has the fields updated in last 24 hours
        """
        # Setup db connection
        self._initialise_db_conn()

        select_query = f"select * from customer_bugs"
        cursor = self.db_conn.cursor()
        cursor.execute(select_query)
        bugs_data = cursor.fetchall()
        for entry in bugs_data:
            # Get the bug id from the db__entry
            b = self.bz_api.getbug(int(entry[1]))

            # Update the fields as per the requirement here
            bug_data = (f'{b.version}', f'{b.target_release[0]}', f'{b.component}', f'{b.severity}', f'{b.assigned_to}', f'{b.qa_contact}', f'{b.status}')
            existing_data = entry[3:]  # ignore the time field
            if bug_data != existing_data:
                print(f"Change seen in Bug: #{b.id}")
                print(existing_data)
                print(bug_data)
                self._update_row_in_db(b, entry[0])
        print("The exiting data is up to date")

    def _append_data_to_db (self, bugs):
        """
        Appends the identified bugs to the database
        Args:
            bugs (bugzilla object): The object containing bugs
        """
        # Enter each bug identified to the db
        if not bugs:
            print("No customer bugs raised in last 24 hours. DB update skipped")

        for bug in bugs:
            # Check if the bug is already present in the db
            select_query = f"select * from {self.table} where bud_id = '{int(bug.id)}'"
            cursor = self.db_conn.cursor()
            cursor.execute(select_query)
            bugs_data = cursor.fetchall()
            if not bugs_data:
                b = self.bz_api.getbug(int(bug.id))
                created_time = b.creation_time.value
                dt = datetime.strptime(str(created_time), '%Y%m%dT%H:%M:%S')
                print("Fetched bug #%s:" % b.id)
                print("  Product            = %s" % b.product)
                print("  Component          = %s" % b.component)
                print("  Status             = %s" % b.status)
                print("  Version            = %s" % b.version)
                print("  Summary            = %s" % b.summary)
                print("  Time               = %s" % dt)
                print("  Severity           = %s" % b.severity)
                print("  Target release     = %s" % b.target_release)
                print("  QA contact         = %s" % b.qa_contact)
                print("  Assignee           = %s" % b.assigned_to)
                cursor.execute("INSERT QUERY HERE")
                self.db_conn.commit()
                print("Records inserted........")
            else:
                print("Record already exists")

    def _update_row_in_db(self, b, dt):

        # Step 1: Delete the row
        self.db_conn.cursor().execute(f"DELETE FROM {self.table} where bud_id = '{b.id}'")

        # Step 2: Insert the updated data into db
        self._insert_into_db(b, dt)

    def _insert_into_db(self, b, dt):
        self.db_conn.cursor().execute("INSERT QUERY HERE")

    def _initialise_api_req(self):
        return bugzilla.Bugzilla(self.url, api_key=self.api_key)

    def _initialise_db_conn(self):
        self.db_conn = psycopg.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        self.db_conn.autocommit = True


obj = FetchCustomerData(url="bugzilla.<org>.com",
                        api_key="API_KEY_HERE",
                        db_name="db_name",
                        db_user="username",
                        db_password="password",
                        db_host="host_ip",
                        db_port="port(def: 5432)",
                        table_name="table_name")
obj.fetch_customer_bugs()
