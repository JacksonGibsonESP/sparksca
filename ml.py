import pandas as pd

from sklearn.model_selection import train_test_split

data = pd.read_csv('glass.csv')

Y = data.Type
X = data.drop(['Type'], axis = 1)
size = 0.2
seed = 10

X_train, X_validation, Y_train, Y_validation = train_test_split(X,Y,test_size=size,random_state=seed)

from sklearn.preprocessing import label_binarize

classes = [1, 2, 3, 5, 6, 7]

Y_train_bin = label_binarize(Y_train, classes=classes)
Y_validation_bin = label_binarize(Y_validation, classes=classes)

from sklearn.linear_model import LogisticRegression

lr = LogisticRegression(C=2.0, solver='lbfgs').fit(X_train, Y_train_bin[:, 0])
res = lr.predict(X_validation)

print(res)



