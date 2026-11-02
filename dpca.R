# Install and load the freqdom.fda package (if needed)
# install.packages("freqdom.fda")
library(freqdom.fda)

# Example: Compute Functional Dynamic Principal Components
# # X is a functional time series object created via freqdom
# X <- sin(seq(0, 2 * pi, length.out = 100)) + rnorm(100, sd = 0.1)
# X <- fts(X, time = seq(0, 2 * pi, length.out = 100))
# # q sets the lag window, freq defines the frequency grid
# res <- fts.dpca(X, q = 30, freq = (-1000:1000/1000) * pi)

# # View dynamic principal component scores and projections
# scores <- fts.dpca.scores(res)
# kl_exp <- fts.dpca.KLexpansion(res)
# var_exp <- fts.dpca.var(res)

# fix the random seed for reproducibility
set.seed(123)

X = rar(100,3)
n_components = 2
lag = 50
# Compute DPCA with only one component
res.dpca = dpca(X, q = lag, Ndpc = n_components)
# Compute PCA with only one component
res.pca = prcomp(X, center = TRUE)
res.pca$x[,-1] = 0
# Reconstruct the data
var.dpca = (1 - sum( (res.dpca$Xhat - X)**2 ) / sum(X**2))*100
var.pca = (1 - sum( (res.pca$x %*% t(res.pca$rotation) - X)**2 ) / sum(X**2))*100
cat("Variance explained by DPCA:\t",var.dpca,"%\n")
cat("Variance explained by PCA:\t",var.pca,"%\n")
