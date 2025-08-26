# Ideas for Improving the Solution

---

## 1. Don't Make Users Wait ⏳

Your `compute.py` script might take a while to run. Instead of making the user wait, run it in the **background**. 

The **API** can instantly respond with a "report started" message and a report ID. The user can use this ID later to check the status and get the finished report.

---

## 2. Use a Stronger Database

Your `store.db` (**SQLite**) is great for getting started, but it can cause errors if many people use your app at once. 

Switching to a database like **PostgreSQL** will make your app more reliable and able to handle more traffic without issues.

# (CSV output file is there present in folders structure only.)