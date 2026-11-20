import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import QualityLedger from '@/views/QualityLedger.vue'
import TransformWorkbench from '@/views/TransformWorkbench.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/transform-workbench', name: 'transform-workbench', component: TransformWorkbench },
    { path: '/quality-ledger', name: 'quality-ledger', component: QualityLedger },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
