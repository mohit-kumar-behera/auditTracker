# Audit Tracker Package

## Installation

```bash
pip install auditTracker
```

## Documentation

### Import out Package

```python
from auditTracker import Tracker
```

**This package is used to track changes made in a file, currently, we only support JSON file format but in the future, we will make it work for more file formats.**

You also need a firebase account for storing the deltas so create a firebase account and take the firebaseConfig as provided by firebase. Make sure to make the accessibility of the storage global and configurations are correct. After that just initialize the storage as shown below.

```python
Tracker.initialize_firebase_storage(firebaseConfig)
```

**FirebaseConfig** is a **dictionary -like** object as show below:-

```python
firebaseConfig = {
   "apiKey": "<your_api_key>",
   "authDomain": "<your_auth_domain>",
   "databaseURL": "<your_databsae_url>",
   "projectId": "<your_project_id>",
   "storageBucket": "<your_storage_bucket>",
   "messagingSenderId": "<your_message_sender_id>",
   "appId": "<your_app_id>",
   "measurementId": "<your_measurement_id>"
}
```

### How to use it

For each file to track you need to initialize an object of the tracker class like so -

```python
trackObj = Tracker(BASE_DIR, audit_filename, table_pk_name)
```

**BASE_DIR:** The path where all the deltas of the file should be stored.

**audit_filename:** What name do you want to give to that file.

**table_pk_name:** primary key of the file, that is any way to uniquely identify each record of the file

Now you are ready to track your files as per your need, now to track any changes made you just need to pass the old and new values of the changes record as shown below.

```python
trackObj.track(oldObj, newObj)
```

**oldObj:** a JSON object containing the information of the previous state of the record that has undergone a change.

**newObj:** a JSON object containing the information of the current state of the record that has undergone a change.

### The different functions needed to get the deltas are described in detail below:

1.  **get_all_audits():** this function will return all the deltas of the file, made till date in a JSON file format.

```python
trackObj.get_all_audits()
```

2.  **audit_of_today():** This function will return the deltas of the file that one has made today in JSON format.

```python
trackObj.audit_of_today()
```

3.  **audit_of_date(d, m, y):** This function will return the deltas of the file that one has made on a particular date, month and year. Here d represents day, m represents month and y represents year in number.

```python
trackObj.audit_of_date(d, m, y)
```

4.  **audit_from_date(d, m, y):** This function will return the deltas in a file that one has made from d/m/y until now. Remember d is the integer that represents day, m represents the month and y represents the year.

```python
trackObj.audit_from_date(d, m, y)
```

5.  **audit_between_date(sd, sm, sy, ed, em, ey, endpoints=False):** This function returns the deltas of the file that has happened between sd/sm/sy to ed/em/ey (sd: starting day, sm: starting month, sy: starting year, ed: ending day, em: ending month, ey: ending year). Endpoints is a parameter that represents whether you want all the deltas or just the final deltas between the endpoints. If endpoints is False(which is its default value) then the function will return all the deltas made between the two dates in a JSON format else if it is True then it will just compare the initial state of the file at sd/sm/sy and compare it with the final state of the file at ed/em/ey and give you the delta changes in a JSON format.

```python
trackObj.audit_between_date(sd, sm, sy, ed, em, ey, endpoints=False)
```

6.  **audit_by_id(id, sd, sm, sy, ed, em, ey, endpoints=False):** Returns the JSON file containing changes made in a particular id between the date ranges sd/sm/sy and ed/em/ey (sd: starting day, sm: starting month, sy: starting year, ed: ending day, em: ending month, ey: ending year).

```python
trackObj.audit_between_date(sd, sm, sy, ed, em, ey, endpoints=False)
```

7.  **audit_by_operation(operation, sd, sm, sy, ed, em, ey):** Returns the JSON file containing the changes made of a particular operation _(updated, inserted, and deleted)_ in the file between sd/sm/sy to ed/em/ey (sd: starting day, sm: starting month, sy: starting year, ed: ending day, em: ending month, ey: ending year).

```python
trackObj.audit_by_operation(operation, sd, sm, sy, ed, em, ey)
```
