/**
 * E2E: Log modal and file explorer.
 *
 * Run with: npx playwright test tests/e2e/log_explorer.spec.ts
 */

import { test, expect } from '@playwright/test';

test.describe('Log Explorer', () => {
  test('modal opens and closes on Escape', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#grid');

    // Try to find a log button (may not exist if no jobs)
    const logBtn = page.locator('.log-btn').first();
    const hasBtns = await logBtn.count() > 0;
    if (!hasBtns) {
      test.skip();
      return;
    }

    await logBtn.click();
    await expect(page.locator('#modal-overlay')).toHaveClass(/open/);
    await page.keyboard.press('Escape');
    await expect(page.locator('#modal-overlay')).not.toHaveClass(/open/);
  });

  test('file tree sections are rendered', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#grid');
    const logBtn = page.locator('.log-btn').first();
    if (await logBtn.count() === 0) { test.skip(); return; }

    await logBtn.click();
    await page.waitForSelector('#tree-pane .tree-section', { timeout: 10000 });
    const sections = page.locator('#tree-pane .tree-section');
    expect(await sections.count()).toBeGreaterThan(0);
  });

  test('clicking a file loads content', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#grid');
    const logBtn = page.locator('.log-btn').first();
    if (await logBtn.count() === 0) { test.skip(); return; }

    await logBtn.click();
    await page.waitForSelector('.tree-item', { timeout: 10000 });
    const item = page.locator('.tree-item').first();
    if (await item.count() === 0) { test.skip(); return; }

    await item.click();
    await page.waitForFunction(
      () => !document.getElementById('modal-content')?.textContent?.includes('Loading'),
      null,
      { timeout: 10000 },
    );
    const content = await page.textContent('#modal-content');
    expect(content).not.toContain('select a file');
  });

  test('source pill shows source type', async ({ page }) => {
    await page.goto('/');
    await page.waitForSelector('#grid');
    const logBtn = page.locator('.log-btn').first();
    if (await logBtn.count() === 0) { test.skip(); return; }

    await logBtn.click();
    await page.waitForSelector('.tree-item', { timeout: 10000 });
    const item = page.locator('.tree-item').first();
    if (await item.count() > 0) {
      await item.click();
      await page.waitForTimeout(2000);
      const source = await page.textContent('#content-source');
      expect(source).toMatch(/source: (local|mount|ssh|cache)/);
    }
  });
});
