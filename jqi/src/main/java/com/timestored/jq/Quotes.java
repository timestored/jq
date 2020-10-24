package com.timestored.jq;

import java.util.concurrent.ThreadLocalRandom;

public class Quotes {
	private static final String[] quotes = new String[] {
			"Talk is cheap. Show me the code. - Linus",
			"Programs must be written for people to read, and only incidentally for machines to execute.",
			"Truth can only be found in one place: the code. ― Robert C. Martin",
			"That's the thing about people who think they hate computers. What they really hate is lousy programmers. - Larry Niven ",
			"A language that doesn't affect the way you think about programming is not worth knowing. ― Alan J. Perlis ",
			"There are two ways of constructing a software design: One way is to make it so simple that there are obviously no deficiencies, and the other way is to make it so complicated that there are no obvious deficiencies. The first method is far more difficult. - C.A.R.Hoare",
			"State the problem before describing the solution - Leslie Lamport",
			"Fred Brooks – The bearing of a child takes nine months, no matter how many women are assigned.",
			"Brian Kernighan – Debugging is twice as hard as writing the code in the first place. Therefore, if you write the code as cleverly as possible, you are, by definition, not smart enough to debug it.",
			"Any fool can write code that a computer can understand. Good programmers write code that humans can understand. ― Martin Fowler ",
			"The best thing about a boolean is even if you are wrong, you are only off by a bit. (Anonymous)",
			"Without requirements or design, programming is the art of adding bugs to an empty text file. (Louis Srygley)",
			"Before software can be reusable it first has to be usable. (Ralph Johnson)",
			"If builders built buildings the way programmers wrote programs, then the first woodpecker that came along would destroy civilization. (Gerald Weinberg)",
			"It’s not a bug – it’s an undocumented feature. (Anonymous)",
			"One man’s crappy software is another man’s full-time job. (Jessica Gaston)",
			"A good programmer is someone who always looks both ways before crossing a one-way street. (Doug Linder)",
			"Always code as if the guy who ends up maintaining your code will be a violent psychopath who knows where you live. (Martin Golding",
			"Walking on water and developing software from a specification are easy if both are frozen. (Edward V Berard)",
			"If debugging is the process of removing software bugs, then programming must be the process of putting them in. (Edsger Dijkstra)",
			"Programming today is a race between software engineers striving to build bigger and better idiot-proof programs, and the universe trying to produce bigger and better idiots. So far, the universe is winning. (Rick Cook)",
			"It’s a curious thing about our industry: not only do we not learn from our mistakes, but we also don’t learn from our successes. (Keith Braithwaite)",
			"There are only two kinds of programming languages: those people always bitch about and those nobody uses. (Bjarne Stroustrup)",
			"In order to understand recursion, one must first understand recursion. (Anonymous)",
			"The cheapest, fastest, and most reliable components are those that aren’t there. (Gordon Bell)",
			"The best performance improvement is the transition from the nonworking state to the working state. (J. Osterhout)",
			"The trouble with programmers is that you can never tell what a programmer is doing until it’s too late. (Seymour Cray)",
			"Don’t worry if it doesn’t work right. If everything did, you’d be out of a job. (Mosher’s Law of Software Engineering)",
			"Java is to JavaScript what car is to Carpet. – Chris Heilmann",
			"Sometimes it pays to stay in bed on Monday, rather than spending the rest of the week debugging Monday’s code. – Dan Salomon",
			"Perfection is achieved not when there is nothing more to add, but rather when there is nothing more to take away. – Antoine de Saint-Exupery",
			"Optimism is an occupational hazard of programming: feedback is the treatment.  Kent Beck",
			"Make it work, make it right, make it fast. – Kent Beck"
	};
	
	public static final String getQuote() {
		return quotes[ThreadLocalRandom.current().nextInt(quotes.length)];
	}
}
