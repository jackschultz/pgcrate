# pgcrate.com (website)

This directory is a minimal static website intended to be deployed to `pgcrate.com`.

## Deployment options

### Option A: GitHub Pages (simple)

1. Create a new repo (recommended): `pgcrate-site` (public).
2. Copy the contents of this `website/` directory into that repo root.
3. In GitHub repo Settings → Pages:
   - Build and deployment: Deploy from a branch
   - Branch: `main` / `/ (root)`
   - Custom domain: `pgcrate.com`
4. Add DNS records at your domain registrar:
   - `A` records for apex (`pgcrate.com`) to GitHub Pages IPs:
     - `185.199.108.153`
     - `185.199.109.153`
     - `185.199.110.153`
     - `185.199.111.153`
   - `CNAME` for `www` → `<your-gh-username>.github.io`
5. Commit a `CNAME` file containing `pgcrate.com` in the site repo root.

### Option B: Vercel / Netlify (more flexible)

- Deploy this folder as a static site.
- Point `pgcrate.com` and `www.pgcrate.com` to the provider per their DNS instructions.

## Content goals

- One-sentence positioning
- Install instructions (`cargo install pgcrate`)
- Links: GitHub, crates.io, docs/help, “Getting started”

