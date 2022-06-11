# fund_price_analysis
Calculate and Analyze the Important Index of Mutual Funds

# TODO:
- [ ] Index Caculation: 
  - [ ] Find sharp ratio of every funds
      - [ ] 1. Fill in the missing nav (take input efficient_selenium repo and save output in /data of this repo)
      - [ ] 2. Calculate 1-week earning curves
      - [ ] 3. Build a simply aleatoric uncertainty model with training data of 1-month
          - [ ] 3.1 Calculate rolling 1-week earning meaning (with each roll considering data of 1-month)
          - [ ] 3.2 Calculate rolling 1-week earning variance (with each roll considering data of 1-month)
          - [ ] 3.3 The rolling mean & variance should be saved in the same table 
      - [ ] 4. From the rolling results, we can estimate the total std
          => total std = (aleatoric variance + epistemic variance) ** 0.5 = (mean of rolling variance + std of rolling means) **0.5
          - [ ] 4.1. Std is the aggregated calculation of the rolling result of (3)
          - [ ] 4.2. Sharp Ratio is the aggregated calculation from last rolling mean of (3) / the last result of (4.1) 
          (NOTE: sharp ratio = last rolling mean / total std)
- [ ] Index Valiation:
    - [ ] 5. Sharp Ratio Comparison (Should Consider the length of 3 to be at least X years)
        - [ ] 5.1. Visualize the length historgram from (3)
        - [ ] 5.2. Plot the relationship of HitRate vs year. (Define HitRate: percentage of earning in the next week is within the range of +-1 std)
        - [ ] 5.3. Find the min length of years for HitRate to be > 0.95. 
- [ ] Try to speed up the rolling mean / std calculation using GPU or simply Numpy
- [ ] Covariance-base index calculation for funds in a company. Ref: https://arxiv.org/pdf/1910.14215.pdf


