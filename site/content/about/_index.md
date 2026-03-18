+++
title = "About"
template = "section.html"
+++

Hey, I'm Yuriy. I build things with Rust.

Citadel started from a simple idea - what if a database encrypted everything by default, without you having to think about it? Not as an afterthought or a plugin, but as the foundation.

Most embedded databases treat encryption as optional. You bolt it on later, hope you configured it right, and pray nothing leaks through the cracks. I wanted something better.

So I built Citadel from scratch - an embedded database engine where every page is encrypted before it hits disk, keys are derived through a proper hierarchy, and the whole thing runs without a WAL using shadow paging.

It's written in Rust, it's open source (MIT/Apache-2.0), and you can try it right now in the [playground](@/demo/_index.md).

If you find it useful, have questions, or want to contribute - find me on <a href="https://github.com/yp3y5akh0v" target="_blank" rel="noopener">GitHub</a>.
