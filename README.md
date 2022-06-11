# fund_price_analysis
Calculate and Analyze the Important Index of Mutual Funds

# TODO:
- [ ] Index Caculation: 
  - [X] Find sharp ratio of every funds
      - [X] 1. Fill in the missing nav (take input efficient_selenium repo and save output in /data of this repo)
      - [X] 2. Calculate 1-week earning curves
      - [X] 3. Build a simply aleatoric uncertainty model with training data of 1-month
          - [X] 3.1 Calculate rolling 1-week earning meaning (with each roll considering data of 1-month)
          - [X] 3.2 Calculate rolling 1-week earning variance (with each roll considering data of 1-month)
          - [X] 3.3 The rolling mean & variance should be saved in the same table 
      - [X] 4. From the rolling results, we can estimate the total std
          => total std = (aleatoric variance + epistemic variance) ** 0.5 = (mean of rolling variance + variance of rolling means) **0.5
          - [X] 4.1. Std is the aggregated calculation of (3)
            - [X] 4.1.1. mean of rolling variance
            - [X] 4.1.2. var of rolling mean
            - [X] 4.1.3. vector sum & square root
          - [X] 4.2. Sharp Ratio is the aggregated calculation from last rolling mean of (3) / the last result of (4.1) 
          (NOTE: sharp ratio = last rolling mean / total std)
- [X] Index Valiation:
    - [X] 5. Sharp Ratio (Should Consider the length of 3 to be at least X years)
        - [X] 5.0. Calculate Hit Rate and Find the earliest plausible sharp ratio & std
        - [-] 5.1. Visualize the length historgram from (3)
        - [-] 5.2. Plot the relationship of HitRate vs year. (Define HitRate: percentage of earning in the next week is within the range of +-1 std)
        - [X] 5.3. Find the min length of years for HitRate to be > 0.95. 
- [ ] Find company with largest amount of funds with plausible index count > 0
- [ ] Find company with largest average sharp ratio
- [ ] Try to speed up the rolling mean / std calculation using GPU or simply Numpy
- [ ] Covariance-base index calculation for funds in a company. Ref: https://arxiv.org/pdf/1910.14215.pdf


