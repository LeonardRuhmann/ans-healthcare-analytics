import { createRouter, createWebHistory } from 'vue-router'
import DashboardView from '../views/DashboardView.vue'

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    routes: [
        {
            path: '/',
            name: 'dashboard',
            component: DashboardView
        },
        {
            path: '/operator/:cnpj',
            name: 'operator-details',
            component: () => import('../views/OperatorDetails.vue')
        }
    ]
})

export default router
