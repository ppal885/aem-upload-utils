// Built into public/app.css by `npm run build:css` (tools/build-css.js); the page no longer compiles Tailwind in the browser.
module.exports = {
    content: ['./public/**/*.html', './public/**/*.js'],
    darkMode: 'class',
    theme: {
        extend: {
            fontFamily: {
                sans: ["'Source Sans 3'", 'Inter', 'system-ui', 'sans-serif'],
                display: ["'Source Sans 3'", 'Inter', 'system-ui', 'sans-serif'],
            },
            colors: {
                adobe: { blue: '#1473E6', darkBlue: '#0D66D0', dark: '#1D1D1D', gray: '#4B4B4B', lightGray: '#F5F5F5' },
            },
        },
    },
};
