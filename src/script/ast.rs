use std::fmt;
use std::num::ParseFloatError;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
	pub line: usize,
	pub col: usize,
}

impl fmt::Display for Location {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", self.line + 1, self.col + 1)
	}
}

#[derive(Debug)]
pub enum LexerError {
	UnexpectedChar(Location, char),
	InvalidNumber(Location, ParseFloatError),
}

pub type LexerResult<T> = Result<T, LexerError>;

#[derive(Debug)]
pub enum Token<'x> {
	ParenOpen(Location),
	ParenClose(Location),
	Quote(Location),
	Symbol(Location, Location, &'x str),
	Number(Location, Location, f64),
}

impl<'x> Token<'x> {
	fn start(&self) -> &Location {
		match self {
			Self::ParenOpen(loc) => loc,
			Self::ParenClose(loc) => loc,
			Self::Quote(loc) => loc,
			Self::Symbol(start, ..) => start,
			Self::Number(start, ..) => start,
		}
	}
}

impl<'x> fmt::Display for Token<'x> {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::ParenOpen(_) => f.write_str("( "),
			Self::ParenClose(_) => f.write_str(")"),
			Self::Quote(_) => f.write_str("'"),
			Self::Symbol(_, _, s) => {
				f.write_str(s)?;
				f.write_str(" ")
			}
			Self::Number(_, _, v) => {
				write!(f, "{}", v)
			}
		}
	}
}

pub struct Lexer<'x> {
	input: &'x str,
	// The location of the next char in the input.
	location: Location,
	had_error: bool,
}

impl<'x> Lexer<'x> {
	pub fn new(input: &'x str) -> Self {
		Self {
			input,
			had_error: false,
			location: Location { line: 0, col: 0 },
		}
	}

	fn location(&self) -> &Location {
		&self.location
	}

	fn skip_whitespace(&mut self) {
		let head = match self.input.find(|c: char| !c.is_whitespace()) {
			Some(v) => {
				let (head, tail) = self.input.split_at(v);
				self.input = tail;
				head
			}
			None => {
				let head = self.input;
				self.input = &""[..];
				head
			}
		};
		// TODO: this breaks with CRLF or CR line endings, but I don't care about those.
		let newlines = head.as_bytes().iter().filter(|b| **b == b'\n').count();
		self.location.line += newlines;
		if newlines == 0 {
			self.location.col += head.chars().count();
		} else {
			// cannot not be None because we found at least one newline earlier
			let last_newline = head.rfind(|c: char| c == '\n').unwrap();
			if last_newline == head.len() {
				self.location.col = 0;
			} else {
				self.location.col = head[last_newline + 1..].chars().count();
			}
		}
	}

	// Note: advance does not do the right thing for newlines!
	fn advance(&mut self, charcount: usize) {
		match self.input.char_indices().nth(charcount) {
			Some((offs, _)) => {
				let (head, tail) = self.input.split_at(offs);
				self.location.col += head.chars().count();
				self.input = tail;
			}
			None => {
				self.location.col += self.input.chars().count();
				self.input = &""[..];
			}
		}
	}

	fn emit<'a>(&mut self, tok: Token<'a>, charcount: usize) -> LexerResult<Token<'a>> {
		self.advance(charcount);
		Ok(tok)
	}

	fn lex_symbol(&mut self) -> LexerResult<Token<'x>> {
		let start = self.location;
		let symbol = match self
			.input
			.find(|c: char| c.is_whitespace() || c == ')' || c == '(')
		{
			Some(end) => {
				let (symbol, tail) = self.input.split_at(end);
				self.input = tail;
				symbol
			}
			None => {
				// the full input is the name, return it
				let mut symbol: &'x str = &""[..];
				std::mem::swap(&mut symbol, &mut self.input);
				symbol
			}
		};
		self.location.col += symbol.chars().count();
		let end = self.location;
		Ok(Token::Symbol(start, end, symbol.into()))
	}

	fn lex_number_or_minus(&mut self) -> LexerResult<Token<'x>> {
		assert_eq!(self.input.as_bytes()[0], b'-');
		if self.input.len() == 1 {
			// minus, because no number digits can follow
			self.lex_symbol()
		} else {
			let mut iter = self.input.chars();
			iter.next().unwrap(); // this is the minus
			let next = iter.next().unwrap(); // this is whatever follows
			if next.is_ascii_digit() {
				self.lex_number()
			} else if next.is_whitespace() {
				// safeguard against typos: we only allow standalone -
				self.lex_symbol()
			} else {
				self.advance(1);
				self.had_error = true;
				Err(LexerError::UnexpectedChar(self.location, next))
			}
		}
	}

	fn lex_number(&mut self) -> LexerResult<Token<'x>> {
		let start = self.location;
		let digits = match self
			.input
			.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-')
		{
			Some(end) => {
				let (digits, tail) = self.input.split_at(end);
				self.input = tail;
				digits
			}
			None => {
				// the full input is the name, return it
				let mut digits: &'x str = &""[..];
				std::mem::swap(&mut digits, &mut self.input);
				digits
			}
		};
		let number = match f64::from_str(digits) {
			Ok(v) => v,
			Err(e) => {
				self.had_error = true;
				return Err(LexerError::InvalidNumber(self.location, e));
			}
		};
		self.location.col += digits.chars().count();
		let end = self.location;
		Ok(Token::Number(start, end, number))
	}
}

impl<'x> Iterator for Lexer<'x> {
	type Item = LexerResult<Token<'x>>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.had_error {
			return None;
		}
		self.skip_whitespace();
		if self.input.len() == 0 {
			return None;
		}

		match self.input.chars().next().unwrap() {
			'(' => Some(self.emit(Token::ParenOpen(self.location), 1)),
			')' => Some(self.emit(Token::ParenClose(self.location), 1)),
			'\'' => Some(self.emit(Token::Quote(self.location), 1)),
			'.' => Some(self.lex_number()),
			'-' => Some(self.lex_number_or_minus()),
			c if c.is_ascii_digit() => Some(self.lex_number()),
			_ => Some(self.lex_symbol()),
		}
	}
}

#[derive(Debug, Clone)]
pub enum ParseError {
	UnexpectedEof(Location),
	UnexpectedChar(Location, char),
	UnexpectedToken(Location, String),
	BareSymbol(Location),
	DoubleQuote(Location),
	QuotedEndOfList(Location),
	InvalidNumber(Location, ParseFloatError),
	MultipleExpressions(Location),
}

impl fmt::Display for ParseError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::UnexpectedEof(loc) => write!(f, "{}: unexpected eof", loc),
			Self::UnexpectedChar(loc, ch) => write!(f, "{}: unexpected char: {}", loc, ch),
			Self::UnexpectedToken(loc, tok) => write!(f, "{}: unexpected token: {}", loc, tok),
			Self::BareSymbol(loc) => write!(f, "{}: bare (quoted) symbol", loc),
			Self::DoubleQuote(loc) => write!(f, "{}: double quote", loc),
			Self::QuotedEndOfList(loc) => write!(f, "{}: quoted end of list", loc),
			Self::InvalidNumber(loc, e) => write!(f, "{}: invalid number: {}", loc, e),
			Self::MultipleExpressions(loc) => {
				write!(f, "{}: more than one expression in source", loc)
			}
		}
	}
}

impl From<LexerError> for ParseError {
	fn from(other: LexerError) -> Self {
		match other {
			LexerError::UnexpectedChar(loc, c) => Self::UnexpectedChar(loc, c),
			LexerError::InvalidNumber(loc, e) => Self::InvalidNumber(loc, e),
		}
	}
}

pub type ParseResult<T> = Result<T, ParseError>;

fn next_or_eof<'x>(lexer: &mut Lexer<'x>) -> ParseResult<Token<'x>> {
	match lexer.next() {
		None => Err(ParseError::UnexpectedEof(*lexer.location())),
		Some(Err(e)) => Err(e.into()),
		Some(Ok(tok)) => Ok(tok),
	}
}

#[derive(Debug)]
pub enum Node<'x> {
	Call(Location, Location, Box<Node<'x>>, Vec<Node<'x>>),
	List(Location, Location, Vec<Node<'x>>),
	Nil(Location),
	Number(Location, f64),
	Ref(Location, &'x str),
}

impl<'x> Node<'x> {
	pub fn parse(mut lexer: Lexer<'x>) -> ParseResult<Self> {
		let node = Self::parse_node(&mut lexer, false)?;
		match lexer.next() {
			Some(Ok(tok)) => return Err(ParseError::MultipleExpressions(*tok.start())),
			Some(Err(e)) => return Err(e.into()),
			None => (),
		};
		Ok(node)
	}

	fn parse_node_or_eol(lexer: &mut Lexer<'x>, quoted: bool) -> ParseResult<Option<Node<'x>>> {
		match next_or_eof(lexer)? {
			Token::ParenOpen(start) => {
				// check if first item is a symbol, if yes, we parse this as a call
				let mut nodes = Self::parse_list(lexer)?;
				if quoted {
					Ok(Some(Node::List(start, *lexer.location(), nodes)))
				} else if nodes.len() == 0 {
					Ok(Some(Node::Nil(start)))
				} else {
					let call_to = nodes.remove(0);
					Ok(Some(Node::Call(
						start,
						*lexer.location(),
						Box::new(call_to),
						nodes,
					)))
				}
			}
			Token::Quote(loc) => {
				if quoted {
					Err(ParseError::DoubleQuote(loc))
				} else {
					Ok(Some(Self::parse_node(lexer, true)?))
				}
			}
			Token::Number(start, _, v) => {
				// quoting doesn't matter
				Ok(Some(Node::Number(start, v)))
			}
			Token::Symbol(start, _, s) => {
				if quoted {
					Err(ParseError::BareSymbol(start))
				} else {
					Ok(Some(Node::Ref(start, s)))
				}
			}
			Token::ParenClose(loc) => {
				if quoted {
					Err(ParseError::QuotedEndOfList(loc))
				} else {
					Ok(None)
				}
			}
		}
	}

	fn parse_node(lexer: &mut Lexer<'x>, quoted: bool) -> ParseResult<Node<'x>> {
		let loc = *lexer.location();
		match Self::parse_node_or_eol(lexer, quoted)? {
			None => Err(ParseError::UnexpectedToken(
				loc,
				Token::ParenClose(loc).to_string(),
			)),
			Some(v) => Ok(v),
		}
	}

	pub fn parse_list(lexer: &mut Lexer<'x>) -> ParseResult<Vec<Self>> {
		let mut nodes = Vec::new();
		while let Some(node) = Self::parse_node_or_eol(lexer, false)? {
			nodes.push(node)
		}
		Ok(nodes)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn do_parse<'x>(s: &'x str) -> ParseResult<Node<'x>> {
		let lexer = Lexer::new(s);
		Node::parse(lexer)
	}

	#[test]
	fn parse_positive_float() {
		match do_parse("23.42") {
			Ok(Node::Number(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, 23.42);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_negative_float() {
		match do_parse("-23.42") {
			Ok(Node::Number(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, -23.42);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_leading_dot_float() {
		match do_parse(".42") {
			Ok(Node::Number(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, 0.42);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_trailing_dot_float() {
		match do_parse("23.") {
			Ok(Node::Number(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, 23.0);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_dotless_number() {
		match do_parse("42") {
			Ok(Node::Number(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, 42f64);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_ref_simple_alphanumeric() {
		match do_parse("foobar2342") {
			Ok(Node::Ref(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, "foobar2342");
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_ref_special_symbols() {
		match do_parse("+/-*") {
			Ok(Node::Ref(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, "+/-*");
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_nil() {
		match do_parse("()") {
			Ok(Node::Nil(loc)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_empty_list() {
		match do_parse("'()") {
			Ok(Node::List(start, end, nodes)) => {
				assert_eq!(start.line, 0);
				assert_eq!(start.col, 1);
				assert_eq!(end.line, 0);
				assert_eq!(end.col, 3);
				assert_eq!(nodes.len(), 0);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_list_of_numbers() {
		match do_parse("'(1 2 3 4)") {
			Ok(Node::List(start, end, nodes)) => {
				assert_eq!(start.line, 0);
				assert_eq!(start.col, 1);
				assert_eq!(end.line, 0);
				assert_eq!(end.col, 10);
				let nums = [1f64, 2., 3., 4.];
				assert_eq!(nodes.len(), 4);
				for (num, node) in (&nums[..]).iter().zip(nodes.iter()) {
					match node {
						Node::Number(_loc, v) => assert_eq!(v, num),
						other => panic!("unexpected node in list: {:?}", other),
					}
				}
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_list_of_symbols() {
		match do_parse("'(x y z)") {
			Ok(Node::List(_start, _end, nodes)) => {
				let syms = ["x", "y", "z"];
				assert_eq!(nodes.len(), 3);
				for (sym, node) in (&syms[..]).iter().zip(nodes.iter()) {
					match node {
						Node::Ref(_loc, s) => assert_eq!(s, sym),
						other => panic!("unexpected node in list: {:?}", other),
					}
				}
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn parse_call() {
		match do_parse("(list 1 2 3 4)") {
			Ok(Node::Call(_start, _end, call_to, nodes)) => {
				match &*call_to {
					Node::Ref(_loc, s) => assert_eq!(*s, "list"),
					_ => panic!("unexpected call atom: {:?}", call_to),
				}

				let nums = [1f64, 2., 3., 4.];
				assert_eq!(nodes.len(), 4);
				for (num, node) in (&nums[..]).iter().zip(nodes.iter()) {
					match node {
						Node::Number(_loc, v) => assert_eq!(v, num),
						_ => panic!("unexpected node in list: {:?}", node),
					}
				}
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn reject_multiple_exprs() {
		match do_parse("23.42 foo") {
			Err(ParseError::MultipleExpressions(loc)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 6);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}

	#[test]
	fn allow_whitespace_after_expr() {
		match do_parse("23.42  ") {
			Ok(Node::Number(loc, v)) => {
				assert_eq!(loc.line, 0);
				assert_eq!(loc.col, 0);
				assert_eq!(v, 23.42);
			}
			other => panic!("unexpected parse result: {:?}", other),
		}
	}
}
