import numpy as np
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt


def plot_confusion_matrix(model, X_test, y_true):
    '''Code stolen from sklearn example.'''
    cm = confusion_matrix(y_true, model.predict(X_test))

    print(cm)

    # Show confusion matrix in a separate window
    plt.matshow(cm)
    plt.title('Confusion matrix')
    plt.colorbar()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.show()

def plot_importance(clf, X, max_features=10):
    '''
    Plot feature importance
    code from lecture
    '''
    feature_importance = clf.feature_importances_
    # make importances relative to max importance
    feature_importance = 100.0 * (feature_importance / feature_importance.max())
    sorted_idx = np.argsort(feature_importance)
    pos = np.arange(sorted_idx.shape[0]) + .5

    # Show only top features
    pos = pos[-max_features:]
    feature_importance = (feature_importance[sorted_idx])[-max_features:]
    feature_names = (X.columns[sorted_idx])[-max_features:]

    plt.barh(pos, feature_importance, align='center')
    plt.yticks(pos, feature_names)
    plt.xlabel('Relative Importance')
    plt.title('Variable Importance')

def ROC_values(score, y):
    '''
    code adopted from stackoverflow
    '''
    roc_x = []
    roc_y = []
    min_score = min(score)
    max_score = max(score)
    thr = np.linspace(min_score, max_score, 30)
    FP=0
    TP=0
    N = sum(y)
    P = len(y) - N

    for (i, T) in enumerate(thr):
        for i in range(0, len(score)):
            if (score[i] > T):
                if (y[i]==1):
                    TP = TP + 1
                if (y[i]==0):
                    FP = FP + 1
        roc_x.append(FP/float(N))
        roc_y.append(TP/float(P))
        FP=0
        TP=0
    return roc_x, roc_y
