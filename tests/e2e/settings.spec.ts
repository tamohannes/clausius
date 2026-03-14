/**
 * E2E: Settings modal, cluster editor, mount controls.
 *
 * Run with: npx playwright test tests/e2e/settings.spec.ts
 */

import { test, expect } from '@playwright/test';

test.describe('Settings', () => {
  test('settings modal opens and closes', async ({ page }) => {
    await page.goto('/');
    await page.click('.nav-user-btn');
    await expect(page.locator('#settings-overlay')).toHaveClass(/open/);
    await page.keyboard.press('Escape');
    await expect(page.locator('#settings-overlay')).not.toHaveClass(/open/);
  });

  test('settings nav sections switch', async ({ page }) => {
    await page.goto('/');
    await page.click('.nav-user-btn');
    await page.click('[data-section="sec-mounts"]');
    await expect(page.locator('#sec-mounts')).toHaveClass(/active/);
    await page.click('[data-section="sec-clusters"]');
    await expect(page.locator('#sec-clusters')).toHaveClass(/active/);
  });

  test('cluster editor shows configured clusters', async ({ page }) => {
    await page.goto('/');
    await page.click('.nav-user-btn');
    await page.click('[data-section="sec-clusters"]');
    await page.waitForTimeout(1000);
    const cards = page.locator('.cluster-edit-card');
    expect(await cards.count()).toBeGreaterThan(0);
  });

  test('add cluster button creates new card', async ({ page }) => {
    await page.goto('/');
    await page.click('.nav-user-btn');
    await page.click('[data-section="sec-clusters"]');
    await page.waitForTimeout(1000);
    const before = await page.locator('.cluster-edit-card').count();
    await page.click('button:has-text("add cluster")');
    const after = await page.locator('.cluster-edit-card').count();
    expect(after).toBe(before + 1);
  });

  test('mount panel shows cluster statuses', async ({ page }) => {
    await page.goto('/');
    await page.click('.nav-user-btn');
    await page.click('[data-section="sec-mounts"]');
    const panel = page.locator('#mount-panel');
    await expect(panel).toBeVisible();
    const items = page.locator('.mount-item');
    expect(await items.count()).toBeGreaterThan(0);
  });
});
