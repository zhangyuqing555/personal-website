const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Serve static assets (CSS, images, etc.)
app.use(express.static(path.join(__dirname, 'public')));

// Route: Home page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// Start server
app.listen(PORT, () => {
  console.log(`✅ Walter Zhang personal website running at http://localhost:${PORT}`);
});
