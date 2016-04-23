# Churn: Good or Bad?
A machine learning system to identifying a portion of the  "good churn" in data from a social discovery and dating app.

##Business Question

Dating apps have the unique problem of loosing the users that find their product most useful. Their churn is not completely bad. Good churn results in word-of-mouth that will result in growth, but how do you identify good churn so that it can be used to improve the product, predict growth and entice investors, partners and users? My project addressed this problem.

##Data

I collaborated with a Social Discovery and Dating App company to do this project. The company provided me with a dump of their PostgreSQL database (28 table containing +200MB of data). Because this project is based on confidential data the data is not included in this repository.

Data included data describing the user (age, sex etc.), records of activity (conections, comments, etc.) and text from public comments. It did not include private conversations between users or good churn labels.

##Approach

My approach to understanding “good churn" was to see if I could predict users that return to the app after a period of inactivity. Presumable these users are happy with the product, but left because they are not interested in dating or meeting new people for a period of time. This analysis does not directly address users that find a long term relationship through the app and stop trying to meet people for a long term, but this analysis could serve as a model if such labels were available through something like a survey.

##Identifying "good churn"

The most challenging part of the analysis was labeling users and determining the time window of the data to be used in the analysis. I considered churn as not submitting comments, liking comments, making connections or sending notifications for a period of two weeks.  

<img src="https://github.com/bprazen/good_churn/blob/master/images/Full_Mean_Activity.png" alt="Mean Activity Histogram" width=500>

The above image is a histogram showing the mean hours between activity for each user. Most users have a mean less than 1 hour. This is due to bursts of activity, where the use does multiple activities when opening the app. This makes activity distributions difficult to interpret.

<img src="https://github.com/bprazen/good_churn/blob/master/images/cumlative_max.png" alt="Maximum time between activities" width=500>

The above image shows the cumulative maximum days between activities for all users. There is a long tail of users that took long breaks, but being gone for 2 weeks is a relatively rare event yet contained enough users to model.

<img src="https://github.com/bprazen/good_churn/blob/master/images/good_churn_diagram.png" alt="Good Churn Diagram" width=500>

The above image illustrates the criteria used to label "good" churn or users that left the app but later returned. These users had a period of 2 weeks without activity and then returned. The other class of users, bad churn, were those that fit this criteria but did not return and have not been active for 200 days.

Because I am combining churn events that happened at different times and users joined the community at different times, I only used data from 2 weeks before each user “churned.”  In other words, I built a model that could be used to predict if a user that has not been active for two weeks will return using data from the two weeks before they left.

##Modeling

<img src="https://github.com/bprazen/good_churn/blob/master/images/pipeline.png" alt="Analysis Pipeline" width=700>

Above is a diagram showing the data analysis pipeline. Text data from user's comments was transformed using Term Frequency–Inverse Document Frequency (TF-IDF) and reduced to 15 variables Singular Value Decomposition (SVD). Records of user activity were summarized in seven features and combined with text features, a count of the number of words in user's comments, the number of comments and their age.

A classification model was built using Random Forest. A number of other ensemble classification techniques performed equally well and the ensemble techniques performed better than the decision trees or logistic regression.

<img src="https://github.com/bprazen/good_churn/blob/master/images/roc.png" alt="roc" width=500>

The above graph is a Receiver Operator Characteristic (ROC) graph summarizing Random Forest model’s ability to correctly classify users from a set of validation data that was not used to build the model. The area under the curve for this model is 0.88.

##Feature Importances

<img src="https://github.com/bprazen/good_churn/blob/master/images/features.png" alt="features" width=500>

The above graph depicts the contribution of the major features to the model. The average time between all activities is the most important feature. Comment length is simple the number of words in users' comments.  “Text Features”  were features built using the words included in users’ comments.
