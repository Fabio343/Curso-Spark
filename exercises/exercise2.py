# # Generate a Random Uniform Number

# Import the `random` module:
import random
import matplotlib.pyplot as plt
import Seaborn as sns
# Generate a random uniform variate in the interval [0, 1):
random.random()

#$\sum_{i=1}{10}$
#$\sum_{i=0}{\frac{10}{e^{n}}}$

#fatorial é dado com base nos multiplicados em sequencia $n!$

m=100
l=[0]*m
for k in range(m):
  l[k]=random.random()
  
l
plt.plot(l)