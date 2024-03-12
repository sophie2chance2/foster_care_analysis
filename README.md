# Foster Care Analysis

## Background

This project is particularly close to my heart. In August of 2023, I became a CASA (Court Appointed Special Advocate) volunteer. I was assigned to a case with a 4 year old, smart, playful, and loving little boy. I have been able to support him and his family towards reunificaiton and have seen the foster care system working.

Since then, my mom, with my support, has taken in a baby boy who was born with drugs in his system and a mother who was not able to care for him. Currently, the court is actively working towards reunification with his mother, but it is not looking promising.

Between these two experiences, I have learned more than I ever thought I would about this system that until a year ago, was completely foreign to me. With this project, I hope to dive deeper into the hope that the foster care system as a whole is supporting children and families in their time of need.

## Data

This data comes from the National Data Archive on Child Abuse and Neglect (NDACAN) and can be found here: https://www.ndacan.acf.hhs.gov/datasets/datasets-list-afcars-foster-care.cfm

I requested the data and received .zip files for the Adoption and Foster Care Analysis and Reporting System (AFCARS), Foster Care Files 2001-2021.

This data is confidential and cannot be shared outside of the team members on this project.

```
National Data Archive on Child Abuse and Neglect (NDACAN). (2023). Adoption and Foster Care Analysis and Reporting System (AFCARS), Foster Care File 2001-2021. National Data Archive on Child Abuse and Neglect (NDACAN). https://doi.org/10.34681/MW23-Q135
```

### Accessing the data

1. If you are a team member, you will need to be granted access to the google cloud bucket.
2. Once you have access, you can access the bucket via gsutil
3. The data is stored in a folder called `foster_care` and is organized by year.
4. Use the `test_google_data_pull.py` file to test that you can access the data.
5. The code assumes that you have a `.env` file with the following variables:

   - `GOOGLE_APPLICATION_CREDENTIALS` = path to your google cloud credentials file

# Defining Success in Foster Care

Intention: Predict the likelihood of a child successfully being reunified with their parent.

- dischargeReason = Reunified with parent, primary caretaker
- Does not enter the system a second time before the age of 18.

#### Entering the System Again

In order to successfully measure this we need to:

1. Calculate the age of each exited ['Exited' = 1] child relative to 2021 (the last year of reporting) and make sure it is at least 18.
2. Check to see if these children appear again in the data set after their exit date.
3. If child never appears again in the data set and their age is above 18 by 2021, they are considered to have successfully exited the system.

### Subquestions

- What is the likelihood of a child who has been reunified with their parent entering the system again?
