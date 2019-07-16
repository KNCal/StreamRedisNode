# Streaming with Redis and NodeJS

This is a demo built using the demo from the RedisConf18 keynote written by Kyle Davis and shows how to use Redis Stream using NodeJS.

This is a technology demo, not a reference architecture nor an example of best practices. While it might be possible to create a usable production system out of the ideas presented, it is not hardened in any way.


## Setup

To setup the demo, you will need Node.js, NPM and your Redis connection information. 

First, install the server-side Node modules with NPM
```
$ npm install
```

The Twitter eater, the `terms` argument is the keyword on which the demo will run - it works best with something high volume (politics, sports, etc). Right now it is hard coded with "trump" as search word.
```
$ node twitter.eater.js
```

The service will launch on localhost:4000


## License

Copyright 2019 Kim Nguyen

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
