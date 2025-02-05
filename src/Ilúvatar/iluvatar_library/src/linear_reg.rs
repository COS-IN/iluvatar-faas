use num_traits::Pow;

#[derive(Debug)]
/// Linear regression calculator
pub struct LinearReg {
    xs: Vec<f64>,
    ys: Vec<f64>,
    slope: f64,
    intercept: f64,
}

impl LinearReg {
    pub fn new() -> Self {
        Self {
            xs: vec![],
            ys: vec![],
            slope: 0.0,
            intercept: 0.0,
        }
    }

    pub fn insert(&mut self, x: f64, y: f64) {
        self.xs.push(x);
        self.ys.push(y);
        let len = self.xs.len() as f64;
        let mut sum_of_x = 0.0;
        let mut sum_of_y = 0.0;
        let mut sum_of_x_by_y = 0.0;
        let mut sum_square_x = 0.0;
        for (x, y) in std::iter::zip(&self.xs, &self.ys) {
            sum_of_x += x;
            sum_of_y += y;
            sum_of_x_by_y += x * y;
            sum_square_x += x.pow(2);
        }

        let div = len * sum_square_x - sum_of_x.pow(2);
        if div != 0.0 {
            self.slope = (len * sum_of_x_by_y - sum_of_x * sum_of_y) / div;
        }
        let div = len * sum_square_x - sum_of_x.pow(2);
        if div != 0.0 {
            self.intercept = (sum_of_y * sum_square_x - sum_of_x * sum_of_x_by_y) / div;
        }
    }
    /// Returns [-1.0] if insufficient data exists for interpolation.
    pub fn predict(&self, x: f64) -> f64 {
        if self.slope != self.intercept && self.intercept != 0.0 {
            self.slope * x + self.intercept
        } else {
            -1.0
        }
    }
}
