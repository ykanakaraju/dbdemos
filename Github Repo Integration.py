# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC ##### Prerequisite
# MAGIC - **Make sure you have created a repository in Github that you want to integrate with Databricks**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Create an access token in your github account.
# MAGIC
# MAGIC   - Open your github account
# MAGIC   - Click on your account icon (top-right icon) and select `Settings` option
# MAGIC   - Click on `Developer settings` option from left side menu (towards the end)
# MAGIC   - Click on `Personal access tokens` -> `Tokens (classic)` from left menu
# MAGIC   - Select `Generate new token` -> `Generate new token (classic)` option from the dropdown (top-right)
# MAGIC   - Create a token with the following options:
# MAGIC     - Name: Demo Token
# MAGIC     - Expiration: 7 days
# MAGIC     - Select scopes: Click on `repo` checkbox
# MAGIC     - Click on `Generate token` button
# MAGIC   - Copy the token and keep it somewhere
# MAGIC     - Token : `<TOKEN>`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create a github linked account in your databricks workspace
# MAGIC
# MAGIC   - Login to your Azure Databricks workspace UI
# MAGIC   - Click on your account icon (top-right icon) and select `Settings` option
# MAGIC   - Click on `Linked accounts` option from the left-side menu
# MAGIC   - Create a Linked account as follows:
# MAGIC     - Git provider : `Github`
# MAGIC     - Select `Personal access token` radio button
# MAGIC     - Git provider username or email: `<Your github account name>`
# MAGIC     - Token: `<TOKEN>`  (from the previous step)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Create a repo in your workspace
# MAGIC
# MAGIC   - Click on `Workspace` menu option and expand select `Repos` option from the workspace
# MAGIC   - Click on `Create` -> `Folder` option and create a folder
# MAGIC   - Click on the folder to navigate into it
# MAGIC   - Click on `Create` -> `Repo` option from the dropdown
# MAGIC   - Create a Repo as follows:
# MAGIC     - Create repo by cloning a Git repository: Check
# MAGIC     - Git repository URL: `<repository url>`   (ex: https://github.com/ykanakaraju/DemoRepo.git )
# MAGIC     - Click on `Create Repo` button to create the repo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Working with your repo
# MAGIC
# MAGIC   - Select the repo you created and add a notebook in it. (add some sample code)
# MAGIC   - Click on `main` branch button next to your repo name
# MAGIC   - Provide some comments and click on `Commit & Push` button to publish the change to your github
# MAGIC   - Click on `main` branch button next to your repo name
# MAGIC   - Click on `Pull` button (top-right) to bring in the files from github into your repo
# MAGIC

# COMMAND ----------


