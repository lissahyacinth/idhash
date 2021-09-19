#[derive(Clone, Copy)]
pub struct IdHashConfigBuilder {
    digits: Option<u32>,
    characters: Option<usize>,
    truncation: Option<usize>,
}

impl Default for IdHashConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IdHashConfigBuilder {
    pub fn new() -> Self {
        IdHashConfigBuilder {
            digits: None,
            characters: None,
            truncation: None,
        }
    }

    pub fn digits(&mut self, x: u32) -> &mut IdHashConfigBuilder {
        self.digits = Some(x);
        self
    }

    pub fn characters(&mut self, x: usize) -> &mut IdHashConfigBuilder {
        self.characters = Some(x);
        self
    }

    pub fn truncation(&mut self, x: usize) -> &mut IdHashConfigBuilder {
        self.truncation = Some(x);
        self
    }

    pub fn build(&self) -> IdHashConfig {
        IdHashConfig {
            digits: if let Some(digits) = self.digits {
                digits
            } else {
                7
            },
            truncation: if let Some(truncation) = self.truncation {
                truncation
            } else {
                128
            },
            characters: if let Some(characters) = self.characters {
                characters
            } else {
                128
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct IdHashConfig {
    pub digits: u32,
    pub truncation: usize,
    pub characters: usize,
}
